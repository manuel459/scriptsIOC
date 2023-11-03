def getData(GLUE_CONTEXT, CONNECTION, P_FECHA_INICIO, P_FECHA_FIN):

    L_SBSIN_INSUNIX =  f'''
                            (
                                (
                                    -----------------------LPG INSUNIX
                                    select	'D' as INDDETREC,
                                            'SBSIN' as TABLAIFRS17,
                                            /*par.cia || '-' || cla.claim*/'' as  PK,
                                            '' as DTPREG,
                                            '' as TIOCPROC, --no disponible
                                            '' as TIOCFRM,
                                            '' as TIOCTO,
                                            'PIG' as KGIORIGM,
                                            'LPG'  as KSCCOMPA,
                                            cast (cla.branch as varchar) as  KGCRAMO,
                                            '' KABPRODT, --ABPRODT.PK
                                            cast (cla.policy as varchar ) DNUMAPO,
                                            cast (cla.certif as varchar ) DNMCERT,
                                            cast (cla.claim as varchar )DNUMSIN ,
                                            cast (cla.occurdat as varchar ) TOCURSIN,
                                            '' as DHSINIST,
                                            cast (cla.occurdat as varchar) TABERSIN,
                                            '' as TPARTSIN,
                                            coalesce (case	when	cla.staclaim in ('1','5','7')
                                                    then	(	select	cast (max(operdate) as varchar )
                                                                from	usinsug01.claim_his
                                                                where	claim = cla.claim
                                                                and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                when	cla.staclaim in ('5') then oper_type in ('11')
                                                                                when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                else	oper_type is null end)
                                                    else null end , '') as TFECHTEC,
                                            coalesce ( case	when	cla.staclaim in ('1','5','7')
                                                    then	(	select	cast (max(operdate) as varchar )
                                                                from	usinsug01.claim_his
                                                                where	claim = cla.claim
                                                                and		case	when	cla.staclaim in ('1') then oper_type in ('3','17')
                                                                                when	cla.staclaim in ('5') then oper_type in ('52')
                                                                                when	cla.staclaim in ('7') then oper_type in ('21')
                                                                                else	oper_type is null end)
                                                    else null end , '') as  TFECHADM,
                                            coalesce ((	select	cast (max(operdate) as varchar)
                                                from	usinsug01.claim_his
                                                where	claim = cla.claim
                                                and		oper_type in
                                                        (	select	cast(codigint as varchar(2))
                                                            from	usinsug01.table140
                                                            where	(	codigint in 
                                                                        (	select	operation
                                                                            from	usinsug01.tab_cl_ope
                                                                            where	(reserve = 1 or ajustes = 1 or pay_amount in (1,3)))) 
                                                                    or codigint in (60))),'') as TESTADO,
                                            cast (cla.staclaim as varchar) as  KSCSITUA,
                                            '' as KSCMOTSI,
                                            '' as KSCTPSIN, --no disponible
                                            cast (cla.causecod as varchar ) as KSCCAUSA,
                                            '' as KSCARGES,
                                            '' as KSCFMPGS, --no disponible
                                            '' as KCBMED_DRA,
                                            '' as KCBMED_PG,
                                            '' as KCBMED_PD,
                                            '' as KCBMED_P2,
                                            cast (coalesce (case	when pol.bussityp = '1' and (cl0.share_coa = 100 and cl0.share_rea = 100) then 1 --LP compa��a l�der, siniestro al 100%
                                                    when pol.bussityp = '1' and (cl0.share_coa <> 100 or cl0.share_rea <> 100) then 2 --LP compa��a l�der, siniestro repartido
                                                    when pol.bussityp = '2' and cl0.share_rea = 100 then 3 --LP compa��a no l�der, siniestro al 100%
                                                    when pol.bussityp = '2' and cl0.share_rea <> 100 then 4 --LP compa��a no l�der, siniestro repartido
                                                    else 0 end, 0)as varchar ) as  KSCTPCSG,
                                            cast (cl0.moneda_cod as varchar ) as  KSCMOEDA,
                                            '' as VCAMBIO, --no disponible
                                            '' as VTXRESPN, --no disponible
                                            cast (cl0.reserva + cl0.ajustes as varchar ) as VMTPROVI,
                                            '' as VMTPRVINI,
                                            cast (((cl0.reserva + cl0.ajustes) * (share_coa/100))  * (share_rea/100) as varchar )  as VMTPRVRS,
                                            cast ((cl0.reserva + cl0.ajustes) * (share_coa/100) as varchar )as  VMTCOSEG,
                                            '' as KSCNATUR,
                                            '' as TALTENAT,
                                            '' as DUSRREG,
                                            cla.client KEBENTID_TO,
                                            cast (coalesce (cl0.capital , 0)as varchar ) as  VCAPITAL,
                                            '' as VTXINDEM, --no disponible
                                            '' as VMTINDEM, --no disponible
                                            '' as TINICIND, --no disponible
                                            '' as DULTACTA,
                                            '' as KACESTAP, --FALTA C�LCULO
                                            '' as DTERMO, 
                                    /*
                                            case	coalesce(cla.certif,0)
                                                    when	0
                                                    then	case	when	coalesce(pol.payfreq,'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                                    else	case	when	coalesce((	select	cer.payfreq
                                                                                        from 	certificat cer
                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                        and 	cer.company = cla.company
                                                                                        and 	cer.certype = pol.certype
                                                                                        and 	cer.branch = cla.branch 
                                                                                        and 	cer.policy = cla.policy
                                                                                        and		cer.certif = cla.certif),'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                                    end DTERMO,
                                    */
                                            '' as TULTALT,
                                            '' as DUSRUPD,
                                            '' as KABUNRIS, --ABUNRIS.PK
                                            '' as DUSRENT,
                                            '' as DUSRSUP,
                                            cla.staclaim as KSCESTSN,
                                            '' as KSCFMPTI,
                                            '' as KSCTPAUT,
                                            '' as KSCLOCSN,
                                            '' as KSCINBMT,
                                            '' as KSCINIDS,
                                            '' as DPROCIDS,
                                            '' as KSCETIDS,
                                            'LPG' as  DCOMPA,
                                            '' as DMARCA,
                                            '' as KSCNATZA_IN,
                                            '' as KSCNATZA_FI,
                                            '' as KSCCDEST,
                                            '' as KACARGES,
                                            '' as DNUMOBJ,
                                            '' as DNUMOB2,
                                            '' as  DUNIDRISC, --ABUNRIS.DUNIRISC
                                            coalesce ((	select	cast (max(operdate) as varchar )
                                                from	usinsug01.claim_his
                                                where	claim = cla.claim
                                                and		oper_type = '16'), '' ) as TDTREABE,
                                            '' as DEQREGUL,
                                            '' as KACMOEST,
                                            '' as KACCONCE,
                                            '' as KSCCONTE,
                                            '' as KSCSTREE, --no disponible
                                            cast (coalesce(	case	when	pol.bussityp = '2'
                                                                then	pol.leadshare
                                                                else	cl0.share_coa
                                                                end,0) as varchar ) VTXCOSEG,
                                            '' as KSCPAIS, --agregar c�lculo
                                            '' as KSCDEFRP,
                                            '' as KSCORPAR,
                                            '' as DTPRCAS,
                                            '' as TDTESTAD, --no disponible
                                            '' as KSBSIN_MP,
                                            '' as TMIGPARA,
                                            '' as KSBSIN_MD,
                                            '' as TMIGDE,
                                            '' as KABAPOL, --ABAPOL.PK
                                            '' as TPRENCER,
                                            '' as TDTREEMB, --no disponible
                                            '' as TENTPLAT,
                                            '' as DHENTPLA,
                                            '' as TENTCOMP,
                                            '' as DHENTCOM,
                                            '' as TPEDPART,
                                            '' as TDTRECLA,
                                            '' as TDECFIN,
                                            '' as TASSRESP,
                                            '' as DINDENCU,
                                            '' as KSCMTENC,
                                            '' as DQTDAAA,
                                            '' as DINFACTO,
                                            '' as TINISUSP,
                                            '' as TFIMSUSP,
                                            '' as DINSOPRE,
                                            '' as KSCTPDAN,
                                            '' as KABAPOL_EFT,
                                            '' as DARQUIVO,
                                            '' as TARQUIVO,
                                            '' as DLOCREF,
                                            '' as KACPARES, --no disponible
                                            '' as KGCRAMO_SAP,
                                            '' as DNUMPGRE, --no disponible
                                            '' as DINDSINTER, --FALTA C�LCULO
                                            '' as DQDREABER,
                                            '' as TPLANOCOSEG,
                                            '' as TPLANORESEG,
                                            cast (coalesce (case	when pol.bussityp = '1' and cl0.share_coa = 100 then 0 --Paga todo
                                                                    when pol.bussityp = '1' and cl0.share_coa <> 100 then 1 --No paga todo
                                                                    when pol.bussityp = '2' then 0 --Paga todo
                                                                    else 0 
                                                            end, 0 ) as varchar) KSCPAGCSG,
                                            --'' as KSCPAGCSG,
                                            '' as KSCAPLGES,
                                            '' as DENTIDSO,
                                            '' as DNOFSIN,
                                            '' as DIMAGEM,
                                            '' as KEBENTID_GS,
                                            '' as KOCSCOPE,
                                            '' as DCDINTTRA
                                    from	(	select	clh.cla_id,
                                                        clh.pol_id,
                                                        clh.capital,
                                                        clh.share_coa,
                                                        clh.share_rea,
                                                        clh.moneda_cod,
                                                        sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                        sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                        sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                from	(	select 	cla.*,
                                                                    tcl.tipo,
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
                                                                                                                                        where clh0.claim = clh.claim
                                                                                                                                        and     clh0.transac =
                                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                                    from    usinsug01.claim_his clh1
                                                                                                                                                    where clh1.claim = clh.claim
                                                                                                                                                    and     clh1.transac <= clh.transac
                                                                                                                                                    and     clh1.exchange not in (1,0)))
                                                                                                                            else    1 end
                                                                                                            else    0 end
                                                                                            else    0 end,0) monto_trans
                                                            from	(	select 	case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                        then	cla.claim
                                                                                        else	null end claim,
                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                        then	cla.ctid 
                                                                                        else	null end cla_id,
                                                                                case	when	pol.certype = '2'
                                                                                        then	pol.ctid
                                                                                        else	null end pol_id,
                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                        then	case	when	exists
                                                                                                                (	select  1
                                                                                                                    from    usinsug01.cover cov
                                                                                                                    join	usinsug01.gen_cover gco
                                                                                                                    on	   	cov.usercomp = cla.usercomp
                                                                                                                    and     cov.company = cla.company
                                                                                                                    and     cov.certype = pol.certype
                                                                                                                    and     cov.branch = cla.branch
                                                                                                                    and     cov.policy = cla.policy
                                                                                                                    and     cov.certif = cla.certif
                                                                                                                    and     cov.effecdate <= cla.occurdat
                                                                                                                    and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                                                                    and     gco.ctid =
                                                                                                                            coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                                                                                    from	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                    and     statregt <> '4' and addsuini ='3'), --variaci�n 3 reg. v�lido
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                    from	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                    and     statregt <> '4' and addsuini ='3')), --variaci�n 3 reg. v�lido
                                                                                                                                                            coalesce((	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                        and     statregt = '4' and addsuini ='3'),
                                                                                                                                                                    (	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                        and     statregt = '4' and addsuini ='3'))), --no est� cortado
                                                                                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                            from	usinsug01.gen_cover
                                                                                                                                                                            where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                            and     statregt <> '4' and addsuini ='3'),
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                            from	usinsug01.gen_cover
                                                                                                                                                                            where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                            and     cover = cov.cover
                                                                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                            and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                            coalesce((	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt = '4' and addsuini ='3'),
                                                                                                                                                                    (   select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt = '4' and addsuini ='3')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                    coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate > cla.occurdat
                                                                                                                                                                        and     statregt <> '4' and addsuini ='3'),
                                                                                                                                                                    (   select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                                                    from	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                    and     statregt = '4' and addsuini ='3'),
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                    from 	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     cover = cov.cover
                                                                                                                                                                    and     effecdate > cla.occurdat
                                                                                                                                                                    and     statregt = '4' and addsuini ='3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                                        then	(	select  max(cov.capital)
                                                                                                                    from    usinsug01.cover cov
                                                                                                                    join 	usinsug01.gen_cover gco
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
                                                                                                                                                                    from	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                    and     statregt <> '4' and addsuini ='3'), --variaci�n 3 reg. v�lido
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                    from	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                    and     statregt <> '4' and addsuini ='3')), --variaci�n 3 reg. v�lido
                                                                                                                                                            coalesce((	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                        and     statregt = '4' and addsuini ='3'),
                                                                                                                                                                    (	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                        and     statregt = '4' and addsuini ='3'))), --no est� cortado
                                                                                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                            from	usinsug01.gen_cover
                                                                                                                                                                            where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                            and     statregt <> '4' and addsuini ='3'),
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                            from	usinsug01.gen_cover
                                                                                                                                                                            where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                            and     cover = cov.cover
                                                                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                            and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                            coalesce((	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt = '4' and addsuini ='3'),
                                                                                                                                                                    (   select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt = '4' and addsuini ='3')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                    coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                        from    usinsug01.gen_cover
                                                                                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate > cla.occurdat
                                                                                                                                                                        and     statregt <> '4' and addsuini ='3'),
                                                                                                                                                                    (   select  max(ctid)
                                                                                                                                                                        from    usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt <> '4' and addsuini ='3')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                                                    from 	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                    and     statregt = '4' and addsuini ='3'),
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                    from 	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     cover = cov.cover
                                                                                                                                                                    and     effecdate > cla.occurdat
                                                                                                                                                                    and     statregt = '4' and addsuini ='3'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                                        else	
                                                                                                                (	select  sum(cov.capital)
                                                                                                                    from    usinsug01.cover cov
                                                                                                                    join	usinsug01.gen_cover gco
                                                                                                                    on	   cov.usercomp = cla.usercomp
                                                                                                                    and     cov.company = cla.company
                                                                                                                    and     cov.certype = pol.certype
                                                                                                                    and     cov.branch = cla.branch
                                                                                                                    and     cov.policy = cla.policy
                                                                                                                    and     cov.certif = cla.certif
                                                                                                                    and     cov.effecdate <= cla.occurdat
                                                                                                                    and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                                                                    and     gco.ctid =
                                                                                                                            coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                                                                                    from	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                    and     statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                    from	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     cover = cov.cover --variaci�n 1 sin modulec
                                                                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                                    and     statregt <> '4' and addsuini = '1')), --variaci�n 3 reg. v�lido
                                                                                                                                                            coalesce((	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                        and     statregt = '4' and addsuini = '1'),
                                                                                                                                                                    (	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                                                        and     statregt = '4' and addsuini = '1'))), --no est� cortado
                                                                                                                                                        coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                            from	usinsug01.gen_cover
                                                                                                                                                                            where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                            and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                            and     statregt <> '4' and addsuini = '1'),
                                                                                                                                                                        (   select  max(ctid)
                                                                                                                                                                            from	usinsug01.gen_cover
                                                                                                                                                                            where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                            and     cover = cov.cover
                                                                                                                                                                            and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                            and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero fue anulado antes del efecto del registro
                                                                                                                                                            coalesce((	select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt = '4' and addsuini = '1'),
                                                                                                                                                                    (   select  max(ctid)
                                                                                                                                                                        from	usinsug01.gen_cover
                                                                                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt = '4' and addsuini = '1')))), --no est� cortado y aparte fue anulado antes del efecto del registro
                                                                                                                                                    coalesce(coalesce((	select  max(ctid)
                                                                                                                                                                        from    usinsug01.gen_cover
                                                                                                                                                                        where 	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                        and     effecdate > cla.occurdat
                                                                                                                                                                        and     statregt <> '4' and addsuini = '1'),
                                                                                                                                                                    (   select  max(ctid)
                                                                                                                                                                        from 	usinsug01.gen_cover
                                                                                                                                                                        where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                        and     cover = cov.cover
                                                                                                                                                                        and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                        and     statregt <> '4' and addsuini = '1')), -- no est� cortado, pero tampoco al efecto de la tabla de datos particular
                                                                                                                                                        coalesce((	select  max(ctid)
                                                                                                                                                                    from 	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     modulec = cov.modulec and cover = cov.cover
                                                                                                                                                                    and     effecdate <= cla.occurdat and nulldate <= cla.occurdat
                                                                                                                                                                    and     statregt = '4' and addsuini = '1'),
                                                                                                                                                                (   select  max(ctid)
                                                                                                                                                                    from 	usinsug01.gen_cover
                                                                                                                                                                    where	usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                                    and     cover = cov.cover
                                                                                                                                                                    and     effecdate > cla.occurdat
                                                                                                                                                                    and     statregt = '4' and addsuini = '1'))))) --est� cortado y no al efecto de la tabla de datos particular adem�s de estar cortado
                                                                                                                                        end *
                                                                                                case	when	cla.branch = 66
                                                                                                        then	(	select	max(exc.exchange)
                                                                                                                    from 	usinsug01.exchange exc
                                                                                                                    where	exc.usercomp = cla.usercomp 
                                                                                                                    and 	exc.company = cla.company 
                                                                                                                    and 	exc.currency = 99
                                                                                                                    and 	exc.effecdate <= cla.occurdat
                                                                                                                    and 	(exc.nulldate is null or exc.nulldate > cla.occurdat))
                                                                                                        else	1 end
                                                                                        else	0 end capital,
                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                        then	case	pol.bussityp
                                                                                                        when	'2'
                                                                                                        then 	100 - coalesce(pol.leadshare,0)
                                                                                                        when	'3'
                                                                                                        then 	null
                                                                                                        else	coalesce((	select	coi.share
                                                                                                                            from	usinsug01.coinsuran coi
                                                                                                                            where   coi.usercomp = cla.usercomp
                                                                                                                            and     coi.company = cla.company
                                                                                                                            and     coi.certype = pol.certype
                                                                                                                            and     coi.branch = cla.branch
                                                                                                                            and     coi.policy = cla.policy
                                                                                                                            and     coi.effecdate <= cla.occurdat
                                                                                                                            and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                            and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                        else	0 end share_coa,
                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                        then	coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                            from    usinsug01.reinsuran rei
                                                                                                            where   rei.usercomp = cla.usercomp
                                                                                                            and     rei.company = cla.company
                                                                                                            and     rei.certype = pol.certype
                                                                                                            and     rei.branch = cla.branch
                                                                                                            and     rei.policy = cla.policy
                                                                                                            and     rei.certif = cla.certif
                                                                                                            and     rei.effecdate <= cla.occurdat
                                                                                                            and     (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                            and     coalesce(rei.type,0) = 1),100)
                                                                                        else	0 end share_rea,
                                                                                case 	when 	(cla.staclaim <> '6' or (cla.branch = 23 and cla.staclaim = '6')) and pol.certype = '2'
                                                                                        then	coalesce(	coalesce((	select	max(cpl.currency)
                                                                                                                        from	usinsug01.curren_pol cpl
                                                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                                                        and 	cpl.company = cla.company
                                                                                                                        and		cpl.certype = pol.certype
                                                                                                                        and		cpl.branch = cla.branch
                                                                                                                        and 	cpl.policy = cla.policy
                                                                                                                        and		cpl.certif = cla.certif),
                                                                                                                    (	select	max(cpl.currency)
                                                                                                                        from	usinsug01.curren_pol cpl
                                                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                                                        and 	cpl.company = cla.company
                                                                                                                        and		cpl.certype = pol.certype
                                                                                                                        and		cpl.branch = cla.branch
                                                                                                                        and 	cpl.policy = cla.policy)),0) 
                                                                                        else	0 end moneda_cod
                                                                        from	usinsug01.policy pol 
                                                                        join	usinsug01.claim cla on cla.usercomp = pol.usercomp
                                                                                                    and 	cla.company = pol.company 
                                                                                                    and 	cla.branch = pol.branch 
                                                                                                    and 	cla.policy = pol.policy
                                                                                                    and		cla.branch not in (66, 23)
                                                                        join    (   select  distinct clh.claim
                                                                                    from    (   select  cast(tcl.operation as varchar(2)) operation
                                                                                                from    usinsug01.tab_cl_ope tcl
                                                                                                where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                                                                    join   usinsug01.claim_his clh
                                                                                    on   coalesce (clh.claim,0) > 0
                                                                                    and     trim(clh.oper_type) = tcl.operation
                                                                                    and     clh.operdate >= '12/31/2021') clh  on clh.claim = cla.claim) cla
                                                                    join usinsug01.claim_his clh on clh.claim = cla.claim
                                                                    join  (	select	case	when tcl.reserve = 1 then 1
                                                                                        when tcl.ajustes = 1 then 2
                                                                                        when tcl.pay_amount = 1 then 3
                                                                                        else 0 end tipo,
                                                                                cast(tcl.operation as varchar(2)) operation
                                                                        from	usinsug01.tab_cl_ope tcl
                                                                        where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) tcl on   	trim(clh.oper_type) = tcl.operation 
                                                                                                                                                and		clh.operdate <= '12/31/2023') clh
                                                                    group	by 1,2,3,4,5,6) cl0
                                    join 	usinsug01.claim cla on cla.ctid = cl0.cla_id
                                    join 	usinsug01.policy pol on pol.ctid = cl0.pol_id
                                    --41.884s <> (ramos 23 y 66)
                                    )
                                    union all 
                                    (
                                    ------------------------LPV INSUNIX

                                    select  'D' as INDDETREC,
                                            'SBSIN' as TABLAIFRS17,
                                            /*par.cia || '-' || cla.claim */ '' as PK,
                                            '' as DTPREG,
                                            '' as TIOCPROC, --no disponible
                                            '' as TIOCFRM,
                                            '' as TIOCTO,
                                            'PIG' as KGIORIGM,
                                            'LPV'  as KSCCOMPA,
                                            cast (cla.branch as varchar) as  KGCRAMO,
                                            '' as KABPRODT, --ABPRODT.PK
                                            cast (cla.policy as varchar) as  DNUMAPO,
                                            cast (cla.certif as varchar ) as  DNMCERT,
                                            cast (cla.claim as varchar ) as  DNUMSIN,
                                            cast (cla.occurdat as varchar) as  TOCURSIN,
                                            '' as DHSINIST,
                                            cast (cla.occurdat as varchar ) as  TABERSIN,
                                            '' as TPARTSIN,
                                            coalesce (case	when	cla.staclaim in ('1','5','7')
                                                    then	(	select	cast (max(operdate) as varchar)
                                                                from	usinsuv01.claim_his
                                                                where	claim = cla.claim
                                                                and		case	when	cla.staclaim in ('1') then trim(oper_type) in ('3','17')
                                                                                when	cla.staclaim in ('5') then trim(oper_type) in ('11')
                                                                                when	cla.staclaim in ('7') then trim(oper_type) in ('21')
                                                                                else	oper_type is null end)
                                                    else null end, '') as  TFECHTEC,
                                            coalesce (case	when	cla.staclaim in ('1','5','7')
                                                    then	(	select	cast (max(operdate)as varchar )
                                                                from	usinsuv01.claim_his
                                                                where	claim = cla.claim
                                                                and		case	when	cla.staclaim in ('1') then trim(oper_type) in ('3','17')
                                                                                when	cla.staclaim in ('5') then trim(oper_type) in ('52')
                                                                                when	cla.staclaim in ('7') then trim(oper_type) in ('21')
                                                                                else	oper_type is null end)
                                                    else null end, '') as  TFECHADM,
                                            coalesce ((	select	cast (max(operdate)as varchar)
                                                from	usinsuv01.claim_his
                                                where	claim = cla.claim
                                                and		trim(oper_type) in
                                                        (	select	cast(codigint as varchar(2))
                                                            from	usinsug01.table140
                                                            where	(	codigint in 
                                                                        (	select	operation
                                                                            from	usinsug01.tab_cl_ope
                                                                            where	(reserve = 1 or ajustes = 1 or pay_amount in (1,3)))) 
                                                                    or codigint in (60))),'') TESTADO,
                                            cla.staclaim as  KSCSITUA,
                                            '' as KSCMOTSI,
                                            ''  as KSCTPSIN, --no disponible
                                            cast (cla.causecod as varchar) KSCCAUSA,
                                            '' as KSCARGES,
                                            '' KSCFMPGS, --no disponible
                                            '' as KCBMED_DRA,
                                            '' as KCBMED_PG,
                                            '' as KCBMED_PD,
                                            '' as KCBMED_P2,
                                            cast (case	when pol.bussityp = '1' and (cl0.share_coa = 100 and cl0.share_rea = 100) then 1 --LP compa��a l�der, siniestro al 100%
                                                    when pol.bussityp = '1' and (cl0.share_coa <> 100 or cl0.share_rea <> 100) then 2 --LP compa��a l�der, siniestro repartido
                                                    when pol.bussityp = '2' and cl0.share_rea = 100 then 3 --LP compa��a no l�der, siniestro al 100%
                                                    when pol.bussityp = '2' and cl0.share_rea <> 100 then 4 --LP compa��a no l�der, siniestro repartido
                                                    else 0 end as varchar ) KSCTPCSG,
                                            cast (cl0.moneda_cod as varchar ) KSCMOEDA,
                                            '' VCAMBIO, --no disponible
                                            '' VTXRESPN, --no disponible
                                            cast (cl0.reserva + cl0.ajustes as varchar ) as VMTPROVI,
                                            '' as VMTPRVINI,
                                            cast ( ((cl0.reserva + cl0.ajustes) * (share_coa/100))  * (share_rea/100) as varchar )  as VMTPRVRS,
                                            cast ((cl0.reserva + cl0.ajustes) * (share_coa/100) as varchar) as  VMTCOSEG,
                                            '' as KSCNATUR,
                                            '' as TALTENAT,
                                            '' as DUSRREG,
                                            cla.client as KEBENTID_TO,
                                            coalesce (cast (cl0.capital as varchar ), '0') as VCAPITAL,
                                            '' as VTXINDEM, --no disponible
                                            '' as VMTINDEM, --no disponible
                                            '' as TINICIND, --no disponible
                                            '' as DULTACTA,
                                            '' as KACESTAP, --FALTA C�LCULO
                                    /*
                                            case	coalesce(cla.certif,0)
                                                    when	0
                                                    then	case	when	coalesce(pol.payfreq,'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                                    else	case	when	coalesce((	select	cer.payfreq
                                                                                        from 	certificat cer
                                                                                        where	cer.usercomp = cla.usercomp 
                                                                                        and 	cer.company = cla.company
                                                                                        and 	cer.certype = pol.certype
                                                                                        and 	cer.branch = cla.branch 
                                                                                        and 	cer.policy = cla.policy
                                                                                        and		cer.certif = cla.certif),'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                                    end DTERMO,
                                    */      '' as DTERMO,
                                            '' as TULTALT,
                                            '' as DUSRUPD,
                                            '' as KABUNRIS, --ABUNRIS.PK
                                            '' as DUSRENT,
                                            '' as DUSRSUP,
                                            cla.staclaim KSCESTSN,
                                            '' as KSCFMPTI,
                                            '' as KSCTPAUT,
                                            '' as KSCLOCSN,
                                            '' as KSCINBMT,
                                            '' as KSCINIDS,
                                            '' as DPROCIDS,
                                            '' as KSCETIDS,
                                            'LPV' DCOMPA,
                                            '' as DMARCA,
                                            '' as KSCNATZA_IN,
                                            '' as KSCNATZA_FI,
                                            '' as KSCCDEST,
                                            '' as KACARGES,
                                            '' as DNUMOBJ,
                                            '' as DNUMOB2,
                                            '' DUNIDRISC, --ABUNRIS.DUNIRISC
                                            coalesce ((	select	cast (max(operdate)as varchar )
                                                        from	usinsuv01.claim_his
                                                        where	claim = cla.claim
                                                        and		oper_type = '16'), '' )as  TDTREABE,
                                            '' as 	DEQREGUL,		
                                            '' as KACMOEST,
                                            '' as KACCONCE,
                                            '' as KSCCONTE,
                                            '' as KSCSTREE, --no disponible
                                            cast (coalesce(	case	when	pol.bussityp = '2'
                                                                then	pol.leadshare
                                                                else	cl0.share_coa
                                                                end,0)as varchar ) as VTXCOSEG,
                                            '' as KSCPAIS, --agregar c�lculo
                                            '' as KSCDEFRP,
                                            '' as KSCORPAR,
                                            '' as DTPRCAS,
                                            '' as TDTESTAD, --no disponible
                                            '' as KSBSIN_MP,
                                            '' as TMIGPARA,
                                            '' as KSBSIN_MD,
                                            '' as TMIGDE,
                                            '' as KABAPOL, --ABAPOL.PK
                                            '' as TPRENCER,
                                            '' as TDTREEMB, --no disponible
                                            '' as TENTPLAT,
                                            '' as DHENTPLA,
                                            '' as TENTCOMP,
                                            '' as DHENTCOM,
                                            '' as TPEDPART,
                                            '' as TDTRECLA,
                                            '' as TDECFIN,
                                            '' as TASSRESP,
                                            '' as DINDENCU,
                                            '' as KSCMTENC,
                                            '' as DQTDAAA,
                                            '' as DINFACTO,
                                            '' as TINISUSP,
                                            '' as TFIMSUSP,
                                            '' as DINSOPRE,
                                            '' as KSCTPDAN,
                                            '' as KABAPOL_EFT,
                                            '' as DARQUIVO,
                                            '' as TARQUIVO,
                                            '' as DLOCREF,
                                            '' as KACPARES, --no disponible
                                            '' as KGCRAMO_SAP,
                                            '' as DNUMPGRE, --no disponible
                                            '' as DINDSINTER, --FALTA C�LCULO
                                            '' as DQDREABER,
                                            '' as TPLANOCOSEG,
                                            '' as TPLANORESEG,
                                            cast ( (case	when pol.bussityp = '1' and cl0.share_coa = 100 then 0 --Paga todo
                                                    when pol.bussityp = '1' and cl0.share_coa <> 100 then 1 --No paga todo
                                                    when pol.bussityp = '2' then 0 --Paga todo
                                                    else 0 end) as varchar ) as KSCPAGCSG,
                                            --'' as KSCPAGCSG,
                                            '' as KSCAPLGES,
                                            '' as DENTIDSO,
                                            '' as DNOFSIN,
                                            '' as DIMAGEM,
                                            '' as KEBENTID_GS,
                                            '' as KOCSCOPE,
                                            '' as DCDINTTRA
                                    from	(	select	clh.cla_id,
                                                        clh.pol_id,
                                                        clh.capital,
                                                        clh.share_coa,
                                                        clh.share_rea,
                                                        clh.moneda_cod,
                                                        sum(clh.monto_trans * case clh.tipo when 1 then 1 else 0 end ) reserva,
                                                        sum(clh.monto_trans * case clh.tipo when 2 then 1 else 0 end ) ajustes,
                                                        sum(clh.monto_trans * case clh.tipo when 3 then 1 else 0 end ) pagos
                                                from	(	select 	cla.*,
                                                                    tcl.tipo,
                                                                    coalesce    (   case    when    clh.currency = cla.moneda_cod
                                                                                            then    clh.amount
                                                                                            when clh.currency <> cla.moneda_cod
                                                                                            then    case    when    cla.moneda_cod = 1
                                                                                                            then    clh.amount *
                                                                                                                    case    when    clh.currency = 2
                                                                                                                            then    (   select  max(clh0.exchange)
                                                                                                                                        from    usinsuv01.claim_his clh0
                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                        and     clh0.transac =
                                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                                    from    usinsuv01.claim_his clh1
                                                                                                                                                    where 	clh1.claim = clh.claim
                                                                                                                                                    and     clh1.transac <= clh.transac
                                                                                                                                                    and     clh1.exchange not in (1,0)))
                                                                                                                            else    1 end
                                                                                                            when cla.moneda_cod = 2
                                                                                                            then    clh.amount /
                                                                                                                    case    when    clh.currency = 1
                                                                                                                            then    (   select  max(clh0.exchange)
                                                                                                                                        from    usinsuv01.claim_his clh0
                                                                                                                                        where	clh0.claim = clh.claim
                                                                                                                                        and     clh0.transac =
                                                                                                                                                (   select  max(clh1.transac)
                                                                                                                                                    from    usinsuv01.claim_his clh1
                                                                                                                                                    where 	clh1.claim = clh.claim
                                                                                                                                                    and     clh1.transac <= clh.transac
                                                                                                                                                    and     clh1.exchange not in (1,0)))
                                                                                                                            else    1 end
                                                                                                            else    0 end
                                                                                            else    0 end,0) monto_trans
                                                            from	(	select 	case 	when 	cla.staclaim = '6' and pol.certype = '2'
                                                                                        then	cla.claim
                                                                                        else	null end claim,
                                                                                case 	when 	cla.staclaim = '6' and pol.certype = '2'
                                                                                        then	cla.ctid 
                                                                                        else	null end cla_id,
                                                                                case	when	pol.certype = '2'
                                                                                        then	pol.ctid
                                                                                        else	null end pol_id,
                                                                                case    when    coalesce((  select  distinct pro.brancht
                                                                                                            from    usinsuv01.product pro
                                                                                                            where pro.usercomp = cla.usercomp
                                                                                                            and     pro.company = cla.company
                                                                                                            and     pro.branch = cla.branch
                                                                                                            and     pro.product = pol.product
                                                                                                            and     pro.effecdate <= cla.occurdat
                                                                                                            and     (pro.nulldate is null or pro.nulldate > cla.occurdat)),'0')
                                                                                                not in ('1','5')
                                                                                        then    (   select  sum(cov.capital)
                                                                                                    from    usinsuv01.cover cov
                                                                                                    join    usinsuv01.gen_cover gco
                                                                                                    on 		cov.usercomp = cla.usercomp
                                                                                                    and     cov.company = cla.company
                                                                                                    and     cov.certype = pol.certype
                                                                                                    and     cov.branch = cla.branch
                                                                                                    and     cov.policy = cla.policy
                                                                                                    and     cov.certif = cla.certif
                                                                                                    and     cov.effecdate <= cla.occurdat
                                                                                                    and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                                                    and     gco.ctid =
                                                                                                            coalesce(coalesce(coalesce(coalesce((   select  max(ctid)
                                                                                                                                                    from 	usinsuv01.gen_cover
                                                                                                                                                    where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                                    and     modulec = cov.modulec and cover = cov.cover --variaci�n 1 con modulec
                                                                                                                                                    and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat) --variaci�n 2 vigencias
                                                                                                                                                    and     statregt <> '4' and addsuini = '1'), --variaci�n 3 reg. v�lido
                                                                                                                                                (   select  max(ctid)
                                                                                                                                                    from 	usinsuv01.gen_cover
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
                                                                                                                                                            from 	usinsuv01.gen_cover
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
                                                                                                                                                    from 	usinsuv01.gen_cover
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
                                                                                                    on 	cov.usercomp = cla.usercomp
                                                                                                    and     cov.company = cla.company
                                                                                                    and     cov.certype = pol.certype
                                                                                                    and     cov.branch = cla.branch
                                                                                                    and     cov.policy = cla.policy
                                                                                                    and     cov.certif = cla.certif
                                                                                                    and     cov.effecdate <= cla.occurdat
                                                                                                    and     (cov.nulldate is null or cov.nulldate > cla.occurdat)
                                                                                                    and     gco.ctid =
                                                                                                            coalesce(coalesce(coalesce((    select  max(ctid)
                                                                                                                                            from 	usinsuv01.life_cover
                                                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                            and     cover = cov.cover
                                                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                            and     statregt <> '4'  and addcapii = '1'), --que no est� cortado
                                                                                                                                        (   select  max(ctid)
                                                                                                                                            from 	usinsuv01.life_cover
                                                                                                                                            where   usercomp = cla.usercomp and company = cla.company and branch = cla.branch and product = pol.product and currency = cov.currency --�ndice regular
                                                                                                                                            and     cover = cov.cover
                                                                                                                                            and     effecdate <= cla.occurdat and (nulldate is null or nulldate > cla.occurdat)
                                                                                                                                            and     statregt = '4'  and addcapii = '1')),--est� cortado
                                                                                                                                    coalesce((  select  max(ctid)
                                                                                                                                                from 	usinsuv01.life_cover
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
                                                                                                                                        and     statregt = '4' and addcapii = '1')))) end capital,
                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                        then	case	pol.bussityp
                                                                                                        when	'2'
                                                                                                        then 	100 - coalesce(pol.leadshare,0)
                                                                                                        when	'3'
                                                                                                        then 	null
                                                                                                        else	coalesce((	select	coi.share
                                                                                                                            from	usinsuv01.coinsuran coi
                                                                                                                            where   coi.usercomp = cla.usercomp
                                                                                                                            and     coi.company = cla.company
                                                                                                                            and     coi.certype = pol.certype
                                                                                                                            and     coi.branch = cla.branch
                                                                                                                            and     coi.policy = cla.policy
                                                                                                                            and     coi.effecdate <= cla.occurdat
                                                                                                                            and     (coi.nulldate is null or coi.nulldate > cla.occurdat)
                                                                                                                            and 	coalesce(coi.companyc,0) in (1,12)),100) end 
                                                                                        else	0 end share_coa,
                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                        then	coalesce((	select  sum(coalesce(rei.share,0))
                                                                                                            from    usinsuv01.reinsuran rei
                                                                                                            where   rei.usercomp = cla.usercomp
                                                                                                            and     rei.company = cla.company
                                                                                                            and     rei.certype = pol.certype
                                                                                                            and     rei.branch = cla.branch
                                                                                                            and     rei.policy = cla.policy
                                                                                                            and     rei.certif = cla.certif
                                                                                                            and     rei.effecdate <= cla.occurdat
                                                                                                            and     (rei.nulldate is null or rei.nulldate > cla.occurdat)
                                                                                                            and     coalesce(rei.type,0) = 1),100)
                                                                                        else	0 end share_rea,
                                                                                case 	when 	cla.staclaim <> '6' and pol.certype = '2'
                                                                                        then	coalesce(	coalesce((	select	max(cpl.currency)
                                                                                                                        from	usinsuv01.curren_pol cpl
                                                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                                                        and 	cpl.company = cla.company
                                                                                                                        and		cpl.certype = pol.certype
                                                                                                                        and		cpl.branch = cla.branch
                                                                                                                        and 	cpl.policy = cla.policy
                                                                                                                        and		cpl.certif = cla.certif),
                                                                                                                    (	select	max(cpl.currency)
                                                                                                                        from	usinsuv01.curren_pol cpl
                                                                                                                        where 	cpl.usercomp = cla.usercomp
                                                                                                                        and 	cpl.company = cla.company
                                                                                                                        and		cpl.certype = pol.certype
                                                                                                                        and		cpl.branch = cla.branch
                                                                                                                        and 	cpl.policy = cla.policy)),0) 
                                                                                        else	0 end moneda_cod
                                                                        from	usinsuv01.policy pol
                                                                        join	usinsuv01.claim cla on 	cla.usercomp = pol.usercomp
                                                                                                    and 	cla.company = pol.company 
                                                                                                    and 	cla.branch = pol.branch 
                                                                                                    and 	cla.policy = pol.policy
                                                                                                    and		cla.branch in (75,32) 
                                                                        join    (   select  distinct clh.claim
                                                                                    from    (   select  cast(tcl.operation as varchar(2)) operation
                                                                                                from    usinsug01.tab_cl_ope tcl
                                                                                                where   (tcl.reserve = 1 or tcl.ajustes = 1)) tcl --solo reservas y ajustes
                                                                                    join    usinsuv01.claim_his clh
                                                                                    on   coalesce (clh.claim,0) > 0
                                                                                    and     trim(clh.oper_type) = tcl.operation
                                                                                    and     clh.operdate >= '12/31/2021') clh on  clh.claim = cla.claim) cla
                                                                    join usinsuv01.claim_his clh on clh.claim = cla.claim
                                                                    join (select	case	when tcl.reserve = 1 then 1
                                                                                        when tcl.ajustes = 1 then 2
                                                                                        when tcl.pay_amount = 1 then 3
                                                                                        else 0 end tipo,
                                                                                cast(tcl.operation as varchar(2)) operation
                                                                        from	usinsug01.tab_cl_ope tcl
                                                                        where	(tcl.reserve = 1 or tcl.ajustes = 1 or tcl.pay_amount = 1)) tcl
                                                            on    	trim(clh.oper_type) = tcl.operation 
                                                            and		clh.operdate <= '12/31/2023') clh
                                                group	by 1,2,3,4,5,6) cl0
                                    join usinsuv01.claim cla on cla.ctid = cl0.cla_id
                                    join usinsuv01.policy pol on pol.ctid = cl0.pol_id
                                    --1.735s <> (75,32)
                                    --1.829s (75,32)
                                    --1.932s (todos)
                                    )

                            ) AS TMP
                            '''
    L_DF_SBSIN_INSUNIX = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_SBSIN_INSUNIX).load()
    
    L_SBSIN_VTIME = f'''
                            (
                                (
                                    ----------------------------------LPG VTIME------------------- 
                                    select	'D' as INDDETREC,
                                            'SBSIN' as TABLAIFRS17,
                                            /*par.cia || par.sep || cla."NCLAIM"*/ '' as PK,
                                            '' as DTPREG,
                                            '' as TIOCPROC, --no disponible
                                            '' as TIOCFRM,
                                            '' as TIOCTO,
                                            'PVG' as KGIORIGM,
                                            'LPG' KSCCOMPA,
                                            cast (cla."NBRANCH" as varchar) as KGCRAMO,
                                            '' as KABPRODT, --ABPRODT.PK
                                            cast (cla."NPOLICY" as varchar ) as  DNUMAPO,
                                            cast (cla."NCERTIF" as varchar ) as  DNMCERT,
                                            cast (cla."NCLAIM" as varchar ) as  DNUMSIN,
                                            cast (cast(cla."DOCCURDAT" as date)as varchar) as TOCURSIN,
                                            '' as DHSINIST,
                                            cast (cast(cla."DOCCURDAT" as date) as varchar) as TABERSIN,
                                            '' as TPARTSIN,
                                            coalesce (case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                    then	(	select	cast (cast(max("DOPERDATE") as date)as varchar )
                                                                from	usvtimg01."CLAIM_HIS"
                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (11)
                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                else	"NOPER_TYPE" is null end)
                                                    else null end, '') as TFECHTEC,
                                            coalesce (case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                    then	(	select	cast (cast(max("DOPERDATE") as date)as varchar)
                                                                from	usvtimg01."CLAIM_HIS"
                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (77)
                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                else	"NOPER_TYPE" is null end)
                                                    else null end, '') as TFECHADM,
                                            coalesce ((	select	cast (cast(max("DOPERDATE") as date) as varchar )
                                                from	usvtimg01."CLAIM_HIS"
                                                where	"NCLAIM" = cla."NCLAIM"
                                                and		"NOPER_TYPE" in
                                                        (	select	"NOPER_TYPE"
                                                            from	usvtimg01."TABLE140"
                                                            where	"NOPER_TYPE" in
                                                                    (	select	cast("SVALUE" as INT4)
                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                        where	"NCONDITION" in (73))
                                                                    or "NOPER_TYPE" in (75,76,77,78,79,80,83))), '') as TESTADO,
                                            trim(cla."SSTACLAIM") as KSCSITUA,
                                            '' as KSCMOTSI,
                                            '' KSCTPSIN, --no disponible
                                            cast (cla."NCAUSECOD" as varchar) as KSCCAUSA,
                                            '' as KSCARGES,
                                            '' as KSCFMPGS, --no disponible
                                            '' as KCBMED_DRA,
                                            '' as KCBMED_PG,
                                            '' as KCBMED_PD,
                                            '' as KCBMED_P2,
                                            cast (coalesce (case	when pol."SBUSSITYP" = '1' and (cl0."NSHARE_COA" = 100 and cl0."NSHARE_REA" = 100) then 1 --LP compa��a l�der, siniestro al 100%
                                                    when pol."SBUSSITYP" = '1' and (cl0."NSHARE_COA" <> 100 or cl0."NSHARE_REA" <> 100) then 2 --LP compa��a l�der, siniestro repartido
                                                    when pol."SBUSSITYP" = '2' and cl0."NSHARE_REA" = 100 then 3 --LP compa��a no l�der, siniestro al 100%
                                                    when pol."SBUSSITYP" = '2' and cl0."NSHARE_REA" <> 100 then 4 --LP compa��a no l�der, siniestro repartido
                                                    else 0 end, 0)as varchar) KSCTPCSG,
                                            cast (cl0."MONEDA_COD" as varchar)  as KSCMOEDA,
                                            '' as VCAMBIO, --no disponible
                                            '' as VTXRESPN, --no disponible
                                            cast (cl0.reserva + cl0.ajustes as varchar)as VMTPROVI,
                                            '' as VMTPRVINI,
                                            cast ( ((cl0.reserva + cl0.ajustes) * ("NSHARE_COA"/100))  * ("NSHARE_REA"/100) as varchar )as  VMTPRVRS,
                                            cast ((cl0.reserva + cl0.ajustes) * ("NSHARE_COA"/100) as varchar ) as  VMTCOSEG,
                                            '' as KSCNATUR,
                                            '' as TALTENAT,
                                            '' as DUSRREG,
                                            cla."SCLIENT" KEBENTID_TO,
                                            cast (cl0."NCAPITAL" as varchar ) as VCAPITAL,
                                            '' VTXINDEM, --no disponible
                                            '' VMTINDEM, --no disponible
                                            '' TINICIND, --no disponible
                                            '' as DULTACTA,
                                            '' KACESTAP, --FALTA C�LCULO
                                            cast (case	coalesce(cla."NCERTIF",0)
                                                    when	0
                                                    then	case	when	coalesce(pol."NPAYFREQ",'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                                    else	case	when	coalesce((	select	cer."NPAYFREQ"
                                                                                        from 	usvtimg01."CERTIFICAT" cer
                                                                                        where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                        and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                        and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                        and		cer."NCERTIF" = cla."NCERTIF"
                                                                                        and		CER."NDIGIT" = 0),'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                            end as varchar )DTERMO,
                                            '' as TULTALT,
                                            '' as DUSRUPD,
                                            '' as KABUNRIS, --ABUNRIS.PK
                                            '' as DUSRENT,
                                            '' as DUSRSUP,
                                            trim(cla."SSTACLAIM") KSCESTSN,
                                            '' as KSCFMPTI,
                                            '' as KSCTPAUT,
                                            '' as KSCLOCSN,
                                            '' as KSCINBMT,
                                            '' as KSCINIDS,
                                            '' as DPROCIDS,
                                            '' as KSCETIDS,
                                            'LPG' as DCOMPA,
                                            '' as DMARCA,
                                            '' as KSCNATZA_IN,
                                            '' as KSCNATZA_FI,
                                            '' as KSCCDEST,
                                            '' as KACARGES,
                                            '' as DNUMOBJ,
                                            '' as DNUMOB2,
                                            '' DUNIDRISC, --ABUNRIS.DUNIRISC
                                            coalesce ( (select	cast (cast(max("DOPERDATE") as date)as VARCHAR)
                                                from	usvtimg01."CLAIM_HIS"
                                                where	"NCLAIM" = cla."NCLAIM"
                                                and		"NOPER_TYPE" = 16),'') as TDTREABE,
                                            '' as DEQREGUL,
                                            '' as KACMOEST,
                                            '' as KACCONCE,
                                            '' as KSCCONTE,
                                            '' KSCSTREE, --no disponible
                                            cast (coalesce(	case	when	pol."SBUSSITYP" = '2'
                                                                then	pol."NLEADSHARE"
                                                                else	cl0."NSHARE_COA"
                                                                end,0)as varchar)as  VTXCOSEG,
                                            '' KSCPAIS, --agregar c�lculo
                                            '' as KSCDEFRP,
                                            '' as KSCORPAR,
                                            '' as DTPRCAS,
                                            '' TDTESTAD, --no disponible
                                            '' as KSBSIN_MP,
                                            '' as TMIGPARA,
                                            '' as KSBSIN_MD,
                                            '' as TMIGDE,
                                            '' KABAPOL, --ABAPOL.PK
                                            '' as TPRENCER,
                                            '' TDTREEMB, --no disponible
                                            '' as TENTPLAT,
                                            '' as DHENTPLA,
                                            '' as TENTCOMP,
                                            '' as DHENTCOM,
                                            '' as TPEDPART,
                                            '' as TDTRECLA,
                                            '' as TDECFIN,
                                            '' as TASSRESP,
                                            '' as DINDENCU,
                                            '' as KSCMTENC,
                                            '' as DQTDAAA,
                                            '' as DINFACTO,
                                            '' as TINISUSP,
                                            '' as TFIMSUSP,
                                            '' as DINSOPRE,
                                            '' as KSCTPDAN,
                                            '' as KABAPOL_EFT,
                                            '' as DARQUIVO,
                                            '' as TARQUIVO,
                                            '' as DLOCREF,
                                            '' KACPARES, --no disponible
                                            '' as KGCRAMO_SAP,
                                            '' DNUMPGRE, --no disponible
                                            '' DINDSINTER, --FALTA C�LCULO
                                            '' as DQDREABER,
                                            '' as TPLANOCOSEG,
                                            '' as TPLANORESEG,
                                            cast (case	when pol."SBUSSITYP" = '1' and cl0."NSHARE_COA" = 100 then 0 --Paga todo
                                                    when pol."SBUSSITYP" = '1' and cl0."NSHARE_COA" <> 100 then 1 --No paga todo
                                                    when pol."SBUSSITYP" = '2' then 0 --Paga todo
                                                    else 0 end as varchar ) KSCPAGCSG,
                                            --'' as KSCPAGCSG,
                                            '' as KSCAPLGES,
                                            '' as DENTIDSO,
                                            '' as DNOFSIN,
                                            '' as DIMAGEM,
                                            '' as KEBENTID_GS,
                                            '' as KOCSCOPE,
                                            '' as DCDINTTRA
                                    from	(	select	clh.cla_id,
                                                        clh.pol_id,
                                                        clh."NCAPITAL",
                                                        clh."NSHARE_COA",
                                                        100 "NSHARE_REA", --PENDIENTE
                                                        clh."MONEDA_COD",
                                                        sum(clh."MONTO_TRANS" * case clh."TIPO" when 1 then 1 else 0 end ) reserva,
                                                        sum(clh."MONTO_TRANS" * case clh."TIPO" when 2 then 1 else 0 end ) ajustes,
                                                        sum(clh."MONTO_TRANS" * case clh."TIPO" when 3 then 1 else 0 end ) pagos
                                                from 	(	select  cla.*,
                                                                    clh."NCASE_NUM",
                                                                    clh."NDEMAN_TYPE",
                                                                    clh."NTRANSAC",
                                                                    clh."NOPER_TYPE",
                                                                    clh."DOPERDATE",
                                                                    csv."TIPO",
                                                                    case	when	cla."MONEDA_COD" = 1
                                                                            then	case	when	clh."NCURRENCY" = 1
                                                                                            then	clh."NAMOUNT"
                                                                                            when	clh."NCURRENCY" = 2
                                                                                            then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                            else	0 end
                                                                            when	cla."MONEDA_COD" = 2
                                                                            then	case	when	clh."NCURRENCY" = 2
                                                                                            then	clh."NAMOUNT"
                                                                                            when	clh."NCURRENCY" = 1
                                                                                            then	clh."NLOC_AMOUNT"
                                                                                            else	0 end
                                                                                    else	0 end "MONTO_TRANS"
                                                            from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                        then    cla."NCLAIM"
                                                                                        else    null end "NCLAIM",
                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                        then    cla.ctid
                                                                                        else    null end cla_id,
                                                                                case    when    pol."SCERTYPE" = '2'
                                                                                        then    pol.ctid
                                                                                        else    null end pol_id,
                                                                                cla."NBRANCH",
                                                                                cla."NPOLICY",
                                                                                cla."NCERTIF",
                                                                                cast(cla."DOCCURDAT" as date) "DOCCURDAT",
                                                                                pol."SCERTYPE",
                                                                                pol."NPRODUCT",
                                                                                pol."SPOLITYPE",
                                                                                case	when	cla."NBRANCH" = 21 
                                                                                        then	(	select	sum(cov."NCAPITAL")
                                                                                                    from    usvtimg01."COVER" cov
                                                                                                    join    usvtimg01."LIFE_COVER" gen
                                                                                                    on   	cov."SCERTYPE"  = pol."SCERTYPE" 
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
                                                                                                    join        usvtimg01."GEN_COVER" gen
                                                                                                    on   	cov."SCERTYPE"  = pol."SCERTYPE" 
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
                                                                                        end "NCAPITAL",
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
                                                                                        else	0 end "NSHARE_COA",
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
                                                                                        else    0 end "MONEDA_COD"
                                                                        from    usvtimg01."POLICY" pol
                                                                        join    usvtimg01."CLAIM" cla 	on cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                        and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                        and     cla."NBRANCH" = pol."NBRANCH"
                                                                        join    (   select  distinct clh."NCLAIM"
                                                                                    from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                from	usvtimg01."CONDITION_SERV" cs 
                                                                                                where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes y pagos
                                                                                    join  usvtimg01."CLAIM_HIS" clh
                                                                                    on   coalesce (clh."NCLAIM",0) > 0
                                                                                    and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                                    and     clh."DOPERDATE" >= '12/31/2021') clh on  clh."NCLAIM" = cla."NCLAIM") cla
                                                                    join usvtimg01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                    join     (	select	case 	when	"NCONDITION" = 71 then 1
                                                                                        when	"NCONDITION" = 72 then 2
                                                                                        when	"NCONDITION" = 73 then 3
                                                                                        else	0 end "TIPO",
                                                                                cast("SVALUE" as INT4) "SVALUE"
                                                                        from	usvtimg01."CONDITION_SERV" cs 
                                                                        where	"NCONDITION" in (71,72,73)) csv --solo reservas y ajustes
                                                            on    clh."NOPER_TYPE" = csv."SVALUE"
                                                            and   clh."DOPERDATE" <= '12/31/2023') clh
                                                group by 1,2,3,4,5,6) cl0
                                    join 	usvtimg01."CLAIM" cla on cla.ctid = cl0.cla_id
                                    join	usvtimg01."POLICY" pol on pol.ctid = cl0.pol_id
                                    ) 
                                    union all 
                                    (
                                    --------------------LPV VTIME----------

                                    select	'D' as INDDETREC,
                                            'SBSIN' as TABLAIFRS17,
                                            /*par.cia || par.sep || cla."NCLAIM"*/ '' as  PK,
                                            '' as DTPREG,
                                            ''  as TIOCPROC, --no disponible
                                            '' as TIOCFRM,
                                            '' as TIOCTO,
                                            '' as KGIORIGM,
                                            'LPV' as KSCCOMPA,
                                            cast (cla."NBRANCH" as varchar ) as KGCRAMO,
                                            '' as  KABPRODT, --ABPRODT.PK
                                            cast (cla."NPOLICY" as varchar ) as DNUMAPO,
                                            cast (cla."NCERTIF" as varchar )as  DNMCERT,
                                            cast (cla."NCLAIM" as varchar ) as DNUMSIN,
                                            cast (cast(cla."DOCCURDAT" as date) as varchar) TOCURSIN,
                                            '' as DHSINIST,
                                            cast (cast(cla."DOCCURDAT" as date) as varchar) TABERSIN,
                                            '' as TPARTSIN,
                                            coalesce ( case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                    then	(	select	cast (cast(max("DOPERDATE") as date) as varchar)
                                                                from	usvtimv01."CLAIM_HIS"
                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (11)
                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                else	"NOPER_TYPE" is null end)
                                                    else null end, '') as TFECHTEC,
                                            coalesce (case	when	trim(cla."SSTACLAIM") in ('1','5','7')
                                                    then	(	select	cast (cast(max("DOPERDATE") as date)as varchar )
                                                                from	usvtimv01."CLAIM_HIS"
                                                                where	"NCLAIM" = cla."NCLAIM"
                                                                and		case	when	trim(cla."SSTACLAIM") in ('1') then "NOPER_TYPE" in (3,17)
                                                                                when	trim(cla."SSTACLAIM") in ('5') then "NOPER_TYPE" in (77)
                                                                                when	trim(cla."SSTACLAIM") in ('7') then "NOPER_TYPE" in (21)
                                                                                else	"NOPER_TYPE" is null end)
                                                    else null end, '') TFECHADM,
                                            coalesce ((	select	cast (cast(max("DOPERDATE") as date)as varchar)
                                                from	usvtimv01."CLAIM_HIS"
                                                where	"NCLAIM" = cla."NCLAIM"
                                                and		"NOPER_TYPE" in
                                                        (	select	"NOPER_TYPE"
                                                            from	usvtimv01."TABLE140"
                                                            where	"NOPER_TYPE" in
                                                                    (	select	cast("SVALUE" as INT4)
                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                        where	"NCONDITION" in (73))
                                                                    or "NOPER_TYPE" in (75,76,77,78,79,80,83))), '' ) as TESTADO,
                                            trim(cla."SSTACLAIM") as KSCSITUA,
                                            '' as KSCMOTSI,
                                            '' KSCTPSIN, --no disponible
                                            cast (cla."NCAUSECOD" as varchar) as  KSCCAUSA,
                                            '' as KSCARGES,
                                            '' as KSCFMPGS, --no disponible
                                            '' as KCBMED_DRA,
                                            '' as KCBMED_PG,
                                            '' as KCBMED_PD,
                                            '' as KCBMED_P2,
                                            cast (case	when pol."SBUSSITYP" = '1' and (cl0."NSHARE_COA" = 100 and cl0."NSHARE_REA" = 100) then 1 --LP compa��a l�der, siniestro al 100%
                                                    when pol."SBUSSITYP" = '1' and (cl0."NSHARE_COA" <> 100 or cl0."NSHARE_REA" <> 100) then 2 --LP compa��a l�der, siniestro repartido
                                                    when pol."SBUSSITYP" = '2' and cl0."NSHARE_REA" = 100 then 3 --LP compa��a no l�der, siniestro al 100%
                                                    when pol."SBUSSITYP" = '2' and cl0."NSHARE_REA" <> 100 then 4 --LP compa��a no l�der, siniestro repartido
                                                    else 0 end as varchar ) KSCTPCSG,
                                            cast (cl0."MONEDA_COD" as varchar ) KSCMOEDA,
                                            '' VCAMBIO, --no disponible
                                            '' VTXRESPN, --no disponible
                                            cast (cl0.reserva + cl0.ajustes as varchar ) as VMTPROVI,
                                            '' as VMTPRVINI,
                                            cast ( ((cl0.reserva + cl0.ajustes) * ("NSHARE_COA"/100))  * ("NSHARE_REA"/100)as varchar ) as  VMTPRVRS,
                                            cast ( (cl0.reserva + cl0.ajustes) * ("NSHARE_COA"/100) as varchar) as  VMTCOSEG,
                                            '' as KSCNATUR,
                                            '' as TALTENAT,
                                            '' as DUSRREG,
                                            cla."SCLIENT" as KEBENTID_TO,
                                            cast (cl0."NCAPITAL" as varchar) as VCAPITAL,
                                            '' VTXINDEM, --no disponible
                                            '' VMTINDEM, --no disponible
                                            '' TINICIND, --no disponible
                                            '' as DULTACTA,
                                            '' KACESTAP, --FALTA C�LCULO
                                            cast (case	coalesce(cla."NCERTIF",0)
                                                    when	0
                                                    then	case	when	coalesce(pol."NPAYFREQ",'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                                    else	case	when	coalesce((	select	cer."NPAYFREQ"
                                                                                        from 	usvtimv01."CERTIFICAT" cer
                                                                                        where	cer."SCERTYPE" = pol."SCERTYPE"
                                                                                        and 	cer."NBRANCH" = cla."NBRANCH"
                                                                                        and 	cer."NPOLICY" = cla."NPOLICY"
                                                                                        and		cer."NCERTIF" = cla."NCERTIF"
                                                                                        and		CER."NDIGIT" = 0),'0') = '1'
                                                                    then	1
                                                                    else	0 end
                                                    end  as varchar ) as DTERMO,
                                            '' as TULTALT,
                                            '' as DUSRUPD,
                                            '' KABUNRIS, --ABUNRIS.PK
                                            '' as DUSRENT,
                                            '' as DUSRSUP,
                                            trim(cla."SSTACLAIM") KSCESTSN,
                                            '' as KSCFMPTI,
                                            '' as KSCTPAUT,
                                            '' as KSCLOCSN,
                                            '' as KSCINBMT,
                                            '' as KSCINIDS,
                                            '' as DPROCIDS,
                                            '' as KSCETIDS,
                                            'LPV' DCOMPA,
                                            '' as DMARCA,
                                            '' as KSCNATZA_IN,
                                            '' as KSCNATZA_FI,
                                            '' as KSCCDEST,
                                            '' as KACARGES,
                                            '' as DNUMOBJ,
                                            '' as DNUMOB2,
                                            '' as DUNIDRISC, --ABUNRIS.DUNIRISC
                                            coalesce ((	select	cast (cast(max("DOPERDATE") as date) as varchar )
                                                from	usvtimg01."CLAIM_HIS"
                                                where	"NCLAIM" = cla."NCLAIM"
                                                and		"NOPER_TYPE" = 16), '') as TDTREABE,
                                            '' as DEQREGUL,
                                            '' as KACMOEST,
                                            '' as KACCONCE,
                                            '' as KSCCONTE,
                                            '' KSCSTREE, --no disponible
                                            cast (coalesce(	case	when	pol."SBUSSITYP" = '2'
                                                                then	pol."NLEADSHARE"
                                                                else	cl0."NSHARE_COA"
                                                                end,0)as varchar ) as  VTXCOSEG,
                                            '' KSCPAIS, --agregar c�lculo
                                            '' as KSCDEFRP,
                                            '' as KSCORPAR,
                                            '' as DTPRCAS,
                                            '' TDTESTAD, --no disponible
                                            '' as KSBSIN_MP,
                                            '' as TMIGPARA,
                                            '' as KSBSIN_MD,
                                            '' as TMIGDE,
                                            '' KABAPOL, --ABAPOL.PK
                                            '' as TPRENCER,
                                            '' TDTREEMB, --no disponible
                                            '' as TENTPLAT,
                                            '' as DHENTPLA,
                                            '' as TENTCOMP,
                                            '' as DHENTCOM,
                                            '' as TPEDPART,
                                            '' as TDTRECLA,
                                            '' as TDECFIN,
                                            '' as TASSRESP,
                                            '' as DINDENCU,
                                            '' as KSCMTENC,
                                            '' as DQTDAAA,
                                            '' as DINFACTO,
                                            '' as TINISUSP,
                                            '' as TFIMSUSP,
                                            '' as DINSOPRE,
                                            '' as KSCTPDAN,
                                            '' as KABAPOL_EFT,
                                            '' as DARQUIVO,
                                            '' as TARQUIVO,
                                            '' as DLOCREF,
                                            '' KACPARES, --no disponible
                                            '' as KGCRAMO_SAP,
                                            '' DNUMPGRE, --no disponible
                                            '' DINDSINTER, --FALTA C�LCULO
                                            '' as DQDREABER,
                                            '' as TPLANOCOSEG,
                                            '' as TPLANORESEG,
                                            cast (case	when pol."SBUSSITYP" = '1' and cl0."NSHARE_COA" = 100 then 0 --Paga todo
                                                    when pol."SBUSSITYP" = '1' and cl0."NSHARE_COA" <> 100 then 1 --No paga todo
                                                    when pol."SBUSSITYP" = '2' then 0 --Paga todo
                                                    else 0 end as varchar ) as  KSCPAGCSG,
                                            --'' as KSCPAGCSG,
                                            '' as KSCAPLGES,
                                            '' as DENTIDSO,
                                            '' as DNOFSIN,
                                            '' as DIMAGEM,
                                            '' as KEBENTID_GS,
                                            '' as KOCSCOPE,
                                            '' as DCDINTTRA	
                                    from	(	select	clh.cla_id,
                                                        clh.pol_id,
                                                        clh."NCAPITAL",
                                                        clh."NSHARE_COA",
                                                        100 "NSHARE_REA",
                                                        clh."MONEDA_COD",
                                                        sum(clh."MONTO_TRANS" * case clh."TIPO" when 1 then 1 else 0 end ) reserva,
                                                        sum(clh."MONTO_TRANS" * case clh."TIPO" when 2 then 1 else 0 end ) ajustes,
                                                        sum(clh."MONTO_TRANS" * case clh."TIPO" when 3 then 1 else 0 end ) pagos
                                                from 	(	select  cla.*,
                                                                    clh."NCASE_NUM",
                                                                    clh."NDEMAN_TYPE",
                                                                    clh."NTRANSAC",
                                                                    clh."NOPER_TYPE",
                                                                    clh."DOPERDATE",
                                                                    csv."TIPO",
                                                                    case	when	cla."MONEDA_COD" = 1
                                                                            then	case	when	clh."NCURRENCY" = 1
                                                                                            then	clh."NAMOUNT"
                                                                                            when	clh."NCURRENCY" = 2
                                                                                            then	clh."NAMOUNT" * clh."NEXCHANGE"
                                                                                            else	0 end
                                                                            when	cla."MONEDA_COD" = 2
                                                                            then	case	when	clh."NCURRENCY" = 2
                                                                                            then	clh."NAMOUNT"
                                                                                            when	clh."NCURRENCY" = 1
                                                                                            then	clh."NLOC_AMOUNT"
                                                                                            else	0 end
                                                                                    else	0 end "MONTO_TRANS"
                                                            from    (   select  case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                        then    cla."NCLAIM"
                                                                                        else    null end "NCLAIM",
                                                                                case    when    cla."SSTACLAIM" <> '6' and pol."SCERTYPE" = '2'
                                                                                        then    cla.ctid
                                                                                        else    null end cla_id,
                                                                                case    when    pol."SCERTYPE" = '2'
                                                                                        then    pol.ctid
                                                                                        else    null end pol_id,
                                                                                cla."NBRANCH",
                                                                                cla."NPOLICY",
                                                                                cla."NCERTIF",
                                                                                cast(cla."DOCCURDAT" as date) "DOCCURDAT",
                                                                                pol."SCERTYPE",
                                                                                pol."NPRODUCT",
                                                                                pol."SPOLITYPE",
                                                                                case	when	cla."NBRANCH" = 21 
                                                                                        then	(	select	sum(cov."NCAPITAL")
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
                                                                                                    and     gen."SADDSUINI" in ('1','3')) --NO HAY '3' EN VTIME
                                                                                        else 	(	select	sum(cov."NCAPITAL")
                                                                                                    from    usvtimv01."COVER" cov
                                                                                                    join        usvtimv01."GEN_COVER" gen
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
                                                                                        end "NCAPITAL",
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
                                                                                        else	0 end "NSHARE_COA",
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
                                                                                        else    0 end "MONEDA_COD"
                                                                        from    usvtimv01."POLICY" pol
                                                                        join    usvtimv01."CLAIM" cla 	on cla."SCERTYPE" = pol."SCERTYPE"
                                                                                                        and     cla."NPOLICY" = pol."NPOLICY"
                                                                                                        and     cla."NBRANCH" = pol."NBRANCH"
                                                                        join    (   select  distinct clh."NCLAIM"
                                                                                    from    (	select	cast("SVALUE" as INT4) "SVALUE"
                                                                                                from	usvtimv01."CONDITION_SERV" cs 
                                                                                                where	"NCONDITION" in (71,72,73)) csv --reservas, ajustes y pagos
                                                                                    join        usvtimv01."CLAIM_HIS" clh
                                                                                    on    coalesce (clh."NCLAIM",0) > 0
                                                                                    and     clh."NOPER_TYPE" = csv."SVALUE"
                                                                                    and     clh."DOPERDATE" >= '12/31/2021') clh  on  clh."NCLAIM" = cla."NCLAIM") cla
                                                                    join usvtimv01."CLAIM_HIS" clh on clh."NCLAIM" = cla."NCLAIM"
                                                                    join (	select	case 	when	"NCONDITION" = 71 then 1
                                                                                        when	"NCONDITION" = 72 then 2
                                                                                        when	"NCONDITION" = 73 then 3
                                                                                        else	0 end "TIPO",
                                                                                cast("SVALUE" as INT4) "SVALUE"
                                                                        from	usvtimv01."CONDITION_SERV" cs 
                                                                        where	"NCONDITION" in (71,72,73)) csv --solo reservas y ajustes
                                                            on      clh."NOPER_TYPE" = csv."SVALUE"
                                                            and     clh."DOPERDATE" <= '12/31/2023') clh
                                                group by 1,2,3,4,5,6) cl0
                                    join 	usvtimv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                    join 	usvtimv01."POLICY" pol on pol.ctid = cl0.pol_id
                                )
                            ) AS TMP
                            '''

    L_DF_SBSIN_VTIME = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_SBSIN_VTIME).load()

    L_SBSIN_INSIS = f'''
                            (
                              
                                select	        'D' as INDDETREC,
                                                'SBSIN' as TABLAIFRS17,
                                                /*par.cia || par.sep || cla."CLAIM_REGID"*/'' as PK,
                                                '' as DTPREG,
                                                '' as TIOCPROC, --no disponible
                                                '' as TIOCFRM,
                                                '' as TIOCTO,
                                                'PNV' as KGIORIGM,
                                                'LPV' as KSCCOMPA,
                                                pol."ATTR1" as KGCRAMO, --PENDIENTE AUN (20231031)
                                                '' as KABPRODT, --ABPRODT.PK
                                                case 	when	cl0.pep_id is not null
                                                                then	coalesce( (select "POLICY_NO" 
                                                                        from usinsiv01."POLICY" 
                                                                        where "POLICY_ID" = cl0.pep_id), '')
                                                                else	pol."POLICY_NO"
                                                end as DNUMAPO,
                                                coalesce (case 	when	cl0.pep_id is not null
                                                                then	pol."POLICY_NO"
                                                                else	null 
                                                                end , '' ) as DNMCERT,
                                                cla."CLAIM_REGID" as DNUMSIN,
                                                cast (cast(cla."EVENT_DATE" as date) as varchar) as TOCURSIN,
                                                '' as DHSINIST,
                                                cast (cast(cla."EVENT_DATE" as date) as varchar) as TABERSIN,
                                                '' as TPARTSIN,
                                                '' as TFECHTEC,
                                                '' as TFECHADM, --PENDIENTE AUN (20231031)
                                                '' as TESTADO, --PENDIENTE AUN (20231031)
                                                '' as KSCSITUA,  --PENDIENTE AUN (20231031)
                                                '' as KSCMOTSI,
                                                '' as KSCTPSIN, --no disponible
                                                '' as KSCCAUSA, --PENDIENTE AUN (20231031)
                                                '' as KSCARGES,
                                                '' as KSCFMPGS, --no disponible
                                                '' as KCBMED_DRA,
                                                '' as KCBMED_PG,
                                                '' as KCBMED_PD,
                                                '' as KCBMED_P2,
                                                '' as KSCTPCSG, --PENDIENTE AUN (20231031)
                                                cast (cl0.moneda_cod as varchar) as KSCMOEDA,
                                                '' as VCAMBIO, --no disponible
                                                '' as VTXRESPN, --no disponible
                                                cast ( cl0.reserva + cl0.ajustes as varchar ) as  VMTPROVI,
                                                '' as VMTPRVINI,
                                                cast ( ((cl0.reserva + cl0.ajustes) * ("NSHARE_COA"/100))  * ("NSHARE_REA"/100) as varchar )as VMTPRVRS,
                                                cast ( (cl0.reserva + cl0.ajustes) * ("NSHARE_COA"/100) as varchar )as VMTCOSEG,
                                                '' as KSCNATUR,
                                                '' as TALTENAT,
                                                '' as DUSRREG,
                                                '' as KEBENTID_TO, --PENDIENTE AUN (20231031)
                                                cast (cl0.ncapital as varchar) as VCAPITAL,
                                                '' as VTXINDEM, --no disponible
                                                '' as VMTINDEM, --no disponible
                                                '' as TINICIND, --no disponible
                                                '' as DULTACTA,
                                                '' as KACESTAP, --FALTA C�LCULO
                                                '' as DTERMO, --PENDIENTE AUN (20231031)
                                                '' as TULTALT,
                                                '' as DUSRUPD,
                                                '' as KABUNRIS, --ABUNRIS.PK
                                                '' as DUSRENT,
                                                '' as DUSRSUP,
                                                '' as KSCESTSN, --PENDIENTE AUN (20231031)
                                                '' as KSCFMPTI,
                                                '' as KSCTPAUT,
                                                '' as KSCLOCSN,
                                                '' as KSCINBMT,
                                                '' as KSCINIDS,
                                                '' as DPROCIDS,
                                                '' as KSCETIDS,
                                                'LPV' as DCOMPA,
                                                '' as DMARCA,
                                                '' as KSCNATZA_IN,
                                                '' as KSCNATZA_FI,
                                                '' as KSCCDEST,
                                                '' as KACARGES,
                                                '' as DNUMOBJ,
                                                '' as DNUMOB2,
                                                '' as DUNIDRISC, --ABUNRIS.DUNIRISC
                                                '' as TDTREABE, --PENDIENTE AUN (20231031)
                                                '' as DEQREGUL,
                                                '' as KACMOEST,
                                                '' as KACCONCE,
                                                '' as KSCCONTE,
                                                '' as KSCSTREE, --no disponible
                                                '' as VTXCOSEG, --PENDIENTE AUN (20231031)
                                                '' as KSCPAIS, --agregar c�lculo
                                                '' as KSCDEFRP,
                                                '' as KSCORPAR,
                                                '' as DTPRCAS,
                                                '' as TDTESTAD, --no disponible
                                                '' as KSBSIN_MP,
                                                '' as TMIGPARA,
                                                '' as KSBSIN_MD,
                                                '' as TMIGDE,
                                                '' as KABAPOL, --ABAPOL.PK
                                                '' as TPRENCER,
                                                '' as TDTREEMB, --no disponible
                                                '' as TENTPLAT,
                                                '' as DHENTPLA,
                                                '' as TENTCOMP,
                                                '' as DHENTCOM,
                                                '' as TPEDPART,
                                                '' as TDTRECLA,
                                                '' as TDECFIN,
                                                '' as TASSRESP,
                                                '' as DINDENCU,
                                                '' as KSCMTENC,
                                                '' as DQTDAAA,
                                                '' as DINFACTO,
                                                '' as TINISUSP,
                                                '' as TFIMSUSP,
                                                '' as DINSOPRE,
                                                '' as KSCTPDAN,
                                                '' as KABAPOL_EFT,
                                                '' as DARQUIVO,
                                                '' as TARQUIVO,
                                                '' as DLOCREF,
                                                '' as KACPARES, --no disponible
                                                '' as KGCRAMO_SAP,
                                                '' as DNUMPGRE, --no disponible
                                                '' as DINDSINTER, --FALTA C�LCULO
                                                '' as DQDREABER,
                                                '' as TPLANOCOSEG,
                                                '' as TPLANORESEG,
                                                '' as KSCPAGCSG, --PENDIENTE AUN (20231031)
                                                --'' as KSCPAGCSG,
                                                '' as KSCAPLGES,
                                                '' as DENTIDSO,
                                                '' as DNOFSIN,
                                                '' as DIMAGEM,
                                                '' as KEBENTID_GS,
                                                '' as KOCSCOPE,
                                                '' as DCDINTTRA
                                from	(	select	cla.cla_id,
                                                                        cla.pol_id,
                                                                        cla.pep_id,
                                                                        cla.moneda_cod,
                                                                        cla.ncapital,
                                                                        100 "NSHARE_COA", --no hay coaseguros en INSIS
                                                                        100 "NSHARE_REA", --PENDIENTE (20231031)
                                                                        sum(case when clh."OP_TYPE" in ('REG') then "RESERV_CHANGE" else 0 end) reserva, --REV.
                                                                        sum(case when clh."OP_TYPE" in ('EST','CLC') then "RESERV_CHANGE" else 0 end) ajustes, --REV.
                                                                        sum(case when clh."OP_TYPE" in ('PAYMCONF','PAYMINV') then "RESERV_CHANGE" else 0 end) pagos --REV.
                                                                        
                                                        from	(	select	cla."CLAIM_ID",
                                                                                                (select "MASTER_POLICY_ID" 
                                                                                                from  usinsiv01."POLICY_ENG_POLICIES" 
                                                                                                where "POLICY_ID" = CLA."POLICY_ID") pep_id,
                                                                                                cla.ctid cla_id,
                                                                                                pol.ctid pol_id,
                                                                                                case   coalesce( 
                                                                                                                (select distinct "AV_CURRENCY"
                                                                                                                                from	usinsiv01."INSURED_OBJECT"
                                                                                                                                where	"POLICY_ID" = pol."POLICY_ID" limit 1
                                                                                                                                ),
                                                                                                                                ''
                                                                                                                                )
                                                                                                                when 'USD' 
                                                                                                                then 2
                                                                                                                when 'PEN' 
                                                                                                                then 1
                                                                                                                else 0 
                                                                                                end MONEDA_COD,
                                                                                        cast(coalesce(( select  sum("INSURED_VALUE")
                                                                                                        from    usinsiv01."INSURED_OBJECT" obj
                                                                                                        where   obj."POLICY_ID" = cla."POLICY_ID"
                                                                                                        and     case  when  not exists
                                                                                                                                (   select  1
                                                                                                                                from    usinsiv01."GEN_ANNEX" ann
                                                                                                                                where   ann."POLICY_ID" = cla."POLICY_ID"
                                                                                                                                and     ann."ANNEX_TYPE" = '17'
                                                                                                                                and     ann."ANNEX_STATE" = '0')
                                                                                                                        then  case  when  cast(pol."INSR_END" as date) > cast(cla."EVENT_DATE" as date) --se busca la prima a la fecha vigente solicitada
                                                                                                                                then  cast(cla."EVENT_DATE" as date) --se busca la prima a la fecha vigente solicitada
                                                                                                                                else  cast(pol."INSR_END" as date) end
                                                                                                                        else  (   select  cast("INSR_BEGIN" as date) -1 --se busca la prima a la fecha de operaci�n anterior a su anulaci�n
                                                                                                                                from    usinsiv01."GEN_ANNEX" ann
                                                                                                                                where   ann."POLICY_ID" = cla."POLICY_ID"
                                                                                                                                and     ann."ANNEX_TYPE" = '17'
                                                                                                                                and     ann."ANNEX_STATE" = '0'
                                                                                                                                and     (ann."INSR_BEGIN" is null or cast(ann."INSR_BEGIN" as date) > cast(cla."EVENT_DATE" as date))) end --se busca la vigencia acorde a la fecha que se solicita
                                                                                                between cast(obj."INSR_BEGIN" as date) and cast(obj."INSR_END" as date)),0) as float) ncapital
                                                                                from	(	select	distinct "CLAIM_ID"
                                                                                                        from	usinsiv01."CLAIM_RESERVE_HISTORY"
                                                                                                        where	"OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                                                                        and		cast("REGISTRATION_DATE" as date) >= '12-31-2021') clh
                                                                                join 	usinsiv01."CLAIM" cla on  cla."CLAIM_ID" = clh."CLAIM_ID"
                                                                                join 	usinsiv01."POLICY" pol on pol."POLICY_ID" = cla."POLICY_ID") cla
                                                        join  usinsiv01."CLAIM_RESERVE_HISTORY" clh
                                                        on	clh."CLAIM_ID" = cla."CLAIM_ID" 
                                                        and clh."OP_TYPE" IN ('REG','EST','CLC','PAYMCONF','PAYMINV')
                                                        and		cast(clh."REGISTRATION_DATE" as date) <= '12-31-2023'
                                                        group	by 1,2,3,4,5) cl0
                                join 	usinsiv01."CLAIM" cla on cla.ctid = cl0.cla_id
                                join 	usinsiv01."POLICY" pol on pol.ctid = cl0.pol_id  
                            ) AS TMP
                            '''
                            
    L_DF_SBSIN_INSIS = GLUE_CONTEXT.read.format('jdbc').options(**CONNECTION).option("dbtable",L_SBSIN_INSIS).load()
  
    L_DF_SBSIN = L_DF_SBSIN_INSUNIX.union(L_DF_SBSIN_VTIME).union(L_DF_SBSIN_INSIS)
    
    return L_DF_SBSIN                      