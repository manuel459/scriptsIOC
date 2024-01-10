def get_data(glue_context, connection):

    L_RBRECPR_INSUNIX_LPG_OTROS_RAMOS = f'''
                                           (
                                               
                                                SELECT 	        'D' as INDDETREC,
                                                                'RBRECPR' as TABLAIFRS17,
                                                                '' PK,
                                                                '' DTPREG, --excluido
                                                                '' TIOCPROC, --excluido
                                                                '' TIOCFRM, --excluido
                                                                '' TIOCTO, --excluido
                                                                '' KRITPREG, --PENDIENTE 01
                                                                'PIG' KGIORIGM, --excluido
                                                                PRE.BRANCH KGCRAMO,
                                                                PRE.POLICY DNUMAPO,
                                                                COALESCE(DPR.CERTIF,0) DNMCERT,
                                                                PRE.RECEIPT DNUMREC,
                                                                '' DNMAGRE, --PENDIENTE 02
                                                                '' NSAGREG, --EXCLUIDO
                                                                '' KEBENTID, --EXCLUIDO
                                                                PRE.ISSUEDAT TEMISSAO, 
                                                                PRE.EFFECDATE TINICIO,
                                                                coalesce (cast (PRE.EXPIRDAT as varchar), '') TTERMO,
                                                                PRE.STATDATE TESTADO,
                                                                PRE.FACTDATE TLIMCOB,
                                                                CASE 	WHEN	PRE.STATUS_PRE = '2'
                                                                                THEN	(	SELECT	 coalesce (cast (MAX(STATDATE) as varchar),'' )
                                                                                        FROM	USINSUG01.PREMIUM_MO
                                                                                        WHERE	USERCOMP = PRE.USERCOMP
                                                                                        AND		COMPANY = PRE.COMPANY
                                                                                        AND		RECEIPT = PRE.RECEIPT
                                                                                        AND		TYPE = 2)
                                                                                ELSE 	'' END TPGCOB, --SIN PERMISOS EN PROD. PREMIUM_MO (1)
                                                                CASE 	WHEN	PRE.STATUS_PRE = '3'
                                                                                THEN	(	SELECT	coalesce (cast (MAX(STATDATE) as varchar), '')
                                                                                        FROM	USINSUG01.PREMIUM_MO
                                                                                        WHERE	USERCOMP = PRE.USERCOMP
                                                                                        AND		COMPANY = PRE.COMPANY
                                                                                        AND		RECEIPT = PRE.RECEIPT
                                                                                        AND		TYPE = '7')
                                                                                ELSE 	'' END TANSUSP, --SIN PERMISOS EN PROD. PREMIUM_MO (2)
                                                                PRE.FACTDATE + 1 TDEVIDO,
                                                                PRE.CURRENCY KRCMOEDA,
                                                                '' VCAMBIO, --DESCARTADO
                                                                DPR.PREMIUM + --PRIMA CONTABLE A NIVEL DEL CERTIFICADO/COBERTURA (NIVEL_1A)
                                                                COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                        AND		DP0.CERTIF = DPR.CERTIF
                                                                        AND		NOT (DP0.TYPE_DETAI IN ('1','3','4') AND DP0.BILL_ITEM NOT IN (4,5,9,97))
                                                                        AND		DP0.BILL_ITEM <> 9),0) + --SE EXCLUYE EL IGV --PRIMA OTROS (NIVEL_1B)
                                                                --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1B)
                                                                CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                        THEN	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                        AND		DP0.CERTIF = 0
                                                                                                        AND		DP0.BILL_ITEM <> 9),0) --SE SUMA LA DITRIBUCIÓN POR LA MATRIZ EN CASO EL CERTIFICADO DPR NO SEA 0 
                                                                                                                        *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0))
                                                                                        ELSE 	0 END VMTCOMR,
                                                                (	DPR.PREMIUM_RECDES + --PRIMA RECARGOS/DESCUENTOS A NIVEL DEL CERTIFICADO/COBERTURA (NIVEL_1A)
                                                                                --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                                --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1B)
                                                                                CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                                THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                        AND		DP0.CERTIF = 0
                                                                                                                        AND		DP0.TYPE_DETAI IN ('3','4') 
                                                                                                                        AND		DP0.BILL_ITEM NOT IN (4,5,9,97)),0)
                                                                                                                        *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                                ELSE 	0 END) +
                                                                        (	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                        AND		DP0.CERTIF = DPR.CERTIF
                                                                                        AND		DP0.BILL_ITEM = 5),0) + --PRIMA DER. EMISIÓN (NIVEL_1)
                                                                                        --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                                        --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1)
                                                                                        CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                                        THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                                AND		DP0.CERTIF = 0
                                                                                                                                AND		DP0.BILL_ITEM = 5),0) --SOLO INTERESA LOS CASOS CON EL MONTO ASOCIADO AL DER. EMISIÓN
                                                                                                                                *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                                        ELSE 	0 END) VMTENCG,
                                                                COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                        AND		DP0.CERTIF = DPR.CERTIF
                                                                        AND		(	(DP0.TYPE_DETAI = '2' AND DP0.TYPE_DETAI IN ('1','3','4') AND DP0.BILL_ITEM NOT IN (4,5,9,97)) 
                                                                                                OR DP0.BILL_ITEM = 9)),0) +--IMPUESTOS('2') E IGV (9) (NIVEL_1)
                                                                        --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                        --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1)
                                                                        CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                        THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                AND		DP0.CERTIF = 0
                                                                                                AND		(	(DP0.TYPE_DETAI = '2' AND DP0.TYPE_DETAI IN ('1','3','4') AND DP0.BILL_ITEM NOT IN (4,5,9,97)) 
                                                                                                                        OR DP0.BILL_ITEM = 9)),0) --IMPUESTOS('2') E IGV (9) (NIVEL_1)
                                                                                                                *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                        ELSE 	0 END VMTIMPO,
                                                                '' VMTPRMPR, --DESCARTADO
                                                                '' VMTPRMBR, --DESCARTADO
                                                                '' VMTPRMTR, --EXCLUIDO
                                                                '' VMTPRMAB, --DESCARTADO
                                                                '' VMTJURO, --PENDIENTE 03
                                                                '' VMTBONU, --EXCLUIDO
                                                                '' VMTDESC, --EXCLUIDO
                                                                '' VMTAGRA, --EXCLUIDO
                                                                DPR.PREMIUM + --PRIMA CONTABLE A NIVEL DEL CERTIFICADO/COBERTURA (NIVEL_1A)
                                                                        COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                AND		DP0.CERTIF = DPR.CERTIF
                                                                                AND		NOT (DP0.TYPE_DETAI IN ('1','3','4') 
                                                                                                AND DP0.BILL_ITEM NOT IN (4,5,9,97))),0) + --PRIMA OTROS (NIVEL_1B)
                                                                        --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                        --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1B)
                                                                        CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                        THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                AND		DP0.CERTIF = 0),0)
                                                                                                                *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                        ELSE 	0 END VMTTOTRP,
                                                                '' VCAPITAL, --EXCLUIDO
                                                                '' KRCFMPGP, --DESCARTADO
                                                                PRE.STATUS_PRE KRCESTRP,
                                                                '' KRCMOSTP, --EXCLUIDO
                                                                PRE.TRATYPEI KRCTPRCP,
                                                                '' KCBMEDCB, --EXCLUIDO
                                                                '' KCBMEDCE, --EXCLUIDO
                                                                '' KCBMEDP2, --EXCLUIDO
                                                                '' KCBMEDRA, --EXCLUIDO
                                                                '' KRCESPRP, --EXCLUIDO
                                                                '' KRCTPCSG, --EXCLUIDO
                                                                '' KCBMEDPD, --EXCLUIDO
                                                                PRE.BRANCH || '-' || PRE.PRODUCT || '-' || 
                                                                        CASE	WHEN	PRE.BRANCH = 23
                                                                                        THEN	COALESCE((	SELECT	SUB_PRODUCT
                                                                                                        FROM	USINSUG01.POL_SUBPRODUCT
                                                                                                        WHERE	USERCOMP = PRE.USERCOMP
                                                                                                        AND 	COMPANY = PRE.COMPANY
                                                                                                        AND		CERTYPE = PRE.CERTYPE
                                                                                                        AND		BRANCH = PRE.BRANCH
                                                                                                        AND		POLICY = PRE.POLICY
                                                                                                        AND		PRODUCT = PRE.PRODUCT),0)
                                                                                        ELSE 	0 END KABPRODT,
                                                                '' KRCTPFRC, --PENDIENTE 04
                                                                COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                AND		DP0.CERTIF = DPR.CERTIF
                                                                                AND		DP0.BILL_ITEM = 4),0) + --PRIMA RGC (NIVEL_1)
                                                                        --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                        --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1)
                                                                        CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                        THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                AND		DP0.CERTIF = 0
                                                                                                                AND		DP0.BILL_ITEM = 4),0) --SOLO INTERESA LOS CASOS CON EL MONTO ASOCIADO AL CONCEPTO RGC
                                                                                                                *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                        ELSE 	0 END VMTCOMCB,
                                                                DPR.COMMISION + --EXISTEN REGISTROS DE PRIMAS CON COMISIÓN
                                                                CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                        THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.COMMISION,0))
                                                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                AND		DP0.CERTIF = 0
                                                                                                                AND		DP0.TYPE_DETAI IN ('1','3','4')
                                                                                                                AND		DP0.BILL_ITEM NOT IN (4,5,9,97)),0) *
                                                                                                                (COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                        ELSE 	0 END VMTCOMMD,
                                                                '' VMTCOMME, --EXCLUIDO
                                                                '' VMTCSAP, --EXCLUIDO
                                                                '' VMTCSCV, --DESCARTADO
                                                                COALESCE((	SELECT	SUM(DP0.PREMIUM)
                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                        AND		DP0.CERTIF = DPR.CERTIF
                                                                        AND		DP0.BILL_ITEM = 97),0) + --PRIMA G. FINANC. (NIVEL_1)
                                                                        --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                        --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1)
                                                                        CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                        THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                AND		DP0.CERTIF = 0
                                                                                                                AND		DP0.BILL_ITEM = 97),0) --SOLO INTERESA LOS CASOS CON EL MONTO ASOCIADO AL DER. EMISIÓN
                                                                                                                *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                                                ELSE 	0 END VMTCSFR,
                                                                '' VMTIMPSL, --DESCARTADO
                                                                '' VMTFAT, --DESCARTADO
                                                                '' VMTFGA, --DESCARTADO
                                                                '' VMTSNB, --DESCARTADO
                                                                '' VMTINEM, --DESCARTADO
                                                                '' VMTFUCA, --DESCARTADO
                                                                (	DPR.PREMIUM + --PRIMA CONTABLE A NIVEL DEL CERTIFICADO/COBERTURA (NIVEL_1A)
                                                                                COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                        AND		DP0.CERTIF = DPR.CERTIF
                                                                                        AND		NOT (DP0.TYPE_DETAI IN ('1','3','4') 
                                                                                        AND DP0.BILL_ITEM NOT IN (4,5,9,97))),0) + --PRIMA OTROS (NIVEL_1B)
                                                                                --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                                --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1B)
                                                                                CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                                THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                        AND		DP0.CERTIF = 0),0)
                                                                                                                        *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                                ELSE 	0 END) *
                                                                                (COALESCE((	SELECT	COALESCE(COI.SHARE,0)
                                                                                        FROM	USINSUG01.COINSURAN COI
                                                                                        WHERE	COI.USERCOMP = PRE.USERCOMP
                                                                                        AND     COI.COMPANY = PRE.COMPANY
                                                                                        AND     COI.CERTYPE = PRE.CERTYPE
                                                                                        AND     COI.BRANCH = PRE.BRANCH
                                                                                        AND     COI.POLICY = PRE.POLICY
                                                                                        AND     COI.EFFECDATE <= PRE.EFFECDATE
                                                                                        AND     (COI.NULLDATE IS NULL OR COI.NULLDATE > PRE.EFFECDATE)
                                                                                        AND 	COALESCE(COI.COMPANYC,0) = 1),100) / 100) VMTCOSEG, --CÁLCULO COASEGURO RETENIDO
                                                                (	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                AND		DP0.CERTIF = DPR.CERTIF
                                                                                AND		DP0.BILL_ITEM = 97),0) + --PRIMA G. FINANC. (NIVEL_1)
                                                                        --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                        --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1)
                                                                        CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                        THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                AND		DP0.CERTIF = 0
                                                                                                                AND		DP0.BILL_ITEM = 97),0) --SOLO INTERESA LOS CASOS CON EL MONTO ASOCIADO AL DER. EMISIÓN
                                                                                                                *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                                                ELSE 	0 END) *
                                                                        (COALESCE((	SELECT	COALESCE(COI.SHARE,0)
                                                                                FROM	USINSUG01.COINSURAN COI
                                                                                WHERE	COI.USERCOMP = PRE.USERCOMP
                                                                                AND     COI.COMPANY = PRE.COMPANY
                                                                                AND     COI.CERTYPE = PRE.CERTYPE
                                                                                AND     COI.BRANCH = PRE.BRANCH
                                                                                AND     COI.POLICY = PRE.POLICY
                                                                                AND     COI.EFFECDATE <= PRE.EFFECDATE
                                                                                AND     (COI.NULLDATE IS NULL OR COI.NULLDATE > PRE.EFFECDATE)
                                                                                AND 	COALESCE(COI.COMPANYC,0) = 1),100) / 100) VMTCSTFC,
                                                                (	DPR.PREMIUM + --PRIMA CONTABLE A NIVEL DEL CERTIFICADO/COBERTURA (NIVEL_1A)
                                                                                COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                        AND		DP0.CERTIF = DPR.CERTIF
                                                                                        AND		NOT (DP0.TYPE_DETAI IN ('1','3','4') 
                                                                                        AND DP0.BILL_ITEM NOT IN (4,5,9,97))),0) + --PRIMA OTROS (NIVEL_1B)
                                                                                --SE PROCEDE A DISTRIBUIR LAS PRIMAS EN EL CERTIFICADO 0 SI ES QUE EL CERTIFICADO EN DPR ES DIFERENTE A 0
                                                                                --(SI EL CERTIFICADO ES 0, YA FUE CALCULADO EN NIVEL_1B)
                                                                                CASE	WHEN	COALESCE(DPR.CERTIF,0) <> 0
                                                                                                THEN	(	COALESCE((	SELECT	SUM(COALESCE(DP0.PREMIUM,0))
                                                                                                                        FROM	USINSUG01.DETAIL_PRE DP0
                                                                                                                        WHERE	DP0.USERCOMP = PRE.USERCOMP
                                                                                                                        AND 	DP0.COMPANY = PRE.COMPANY
                                                                                                                        AND 	DP0.RECEIPT = PRE.RECEIPT
                                                                                                                        AND		DP0.CERTIF = 0),0)
                                                                                                                        *	(COALESCE(DPR.PREMIUM / NULLIF(SUM(DPR.PREMIUM) OVER (PARTITION BY DPR.PRE_ID),0),0)))
                                                                                                ELSE 	0 END) *
                                                                                (COALESCE((	SELECT	COALESCE(COI.SHARE,0)
                                                                                        FROM	USINSUG01.COINSURAN COI
                                                                                        WHERE	COI.USERCOMP = PRE.USERCOMP
                                                                                        AND     COI.COMPANY = PRE.COMPANY
                                                                                        AND     COI.CERTYPE = PRE.CERTYPE
                                                                                        AND     COI.BRANCH = PRE.BRANCH
                                                                                        AND     COI.POLICY = PRE.POLICY
                                                                                        AND     COI.EFFECDATE <= PRE.EFFECDATE
                                                                                        AND     (COI.NULLDATE IS NULL OR COI.NULLDATE > PRE.EFFECDATE)
                                                                                        AND 	COALESCE(COI.COMPANYC,0) = 1),100) / 100) * --CÁLCULO COASEGURO RETENIDO
                                                                                (	COALESCE((	SELECT  SUM(COALESCE(REI.SHARE,0)) --CÁLCULO REASEGURO RETENIDO
                                                                                                FROM    USINSUG01.REINSURAN REI
                                                                                                WHERE   REI.USERCOMP = PRE.USERCOMP
                                                                                                AND     REI.COMPANY = PRE.COMPANY
                                                                                                AND     REI.CERTYPE = PRE.CERTYPE
                                                                                                AND     REI.BRANCH = PRE.BRANCH
                                                                                                AND     REI.POLICY = PRE.POLICY
                                                                                                AND     REI.CERTIF = COALESCE(DPR.CERTIF,0)
                                                                                                AND     REI.EFFECDATE <= PRE.EFFECDATE
                                                                                                AND     (REI.NULLDATE IS NULL OR REI.NULLDATE > PRE.EFFECDATE)
                                                                                                AND     COALESCE(REI.TYPE,0) = 1),100) / 100) VMTRESSG,
                                                                '' VMTCMCCS, --DESCARTADO
                                                                '' DUSRUPD, --EXCLUIDO
                                                                '' VMTCOMFD, --DESCARTADO
                                                                '' VMTCOMPG, --DESCARTADO
                                                                'LPG' DCOMPA,
                                                                '' DMARCA, --EXCLUIDO
                                                                '' KRBRECPR_MP, --EXCLUIDO
                                                                '' TMIGPARA, --EXCLUIDO
                                                                '' KRBRECPR_MD, --EXCLUIDO
                                                                '' TMIGDE, --EXCLUIDO
                                                                PRE.BRANCH || '-' || PRE.POLICY || '-' || COALESCE(DPR.CERTIF,0) KABAPOL,
                                                                '' KABAPOL_EFT, --EXCLUIDO
                                                                '' KABAPOL_GRP, --EXCLUIDO
                                                                '' DNUMPRES, --PENDIENTE 05
                                                                COALESCE((	SELECT 	ACC.BRANCH_BAL
                                                                                        FROM	USINSUG01.ACC_AUTOM2 ACC
                                                                                        WHERE	CTID =
                                                                                                        COALESCE(
                                                                                                (   SELECT  MIN(CTID) --BÚSQUEDA NORMAL EN ACC_AUTOM2(RAMO;PRODUCTO;CONCEPTO 1 PARA TODOS, EXCEPTO INCENDIO)
                                                                                                FROM    USINSUG01.ACC_AUTOM2 ABE
                                                                                                WHERE   ABE.BRANCH = PRE.BRANCH
                                                                                                AND 	ABE.PRODUCT = PRE.PRODUCT
                                                                                                AND 	ABE.CONCEPT_FAC = 1),
                                                                                                (   SELECT  MIN(CTID) --BÚSQUEDA IGUAL A ANTERIOR, PERO EN CASO EL PRODUCTO NO EXISTA EN ACC_AUTOM2 (ERROR LP)
                                                                                                FROM    USINSUG01.ACC_AUTOM2 ABE
                                                                                                WHERE   ABE.BRANCH = PRE.BRANCH
                                                                                                AND 	ABE.CONCEPT_FAC = 1))),0) KGCRAMO_SAP,
                                                                '' KRCRGMDL, --EXCLUIDO
                                                                '' DNIB, --EXCLUIDO
                                                                '' DREFEATM, --EXCLUIDO
                                                                '' DINDRESG, --EXCLUIDO
                                                                '' TINICIOA, --EXCLUIDO
                                                                '' TTERMOA, --EXCLUIDO
                                                                '' DTERMO, --EXCLUIDO
                                                                '' DDEUNRIS, --EXCLUIDO
                                                                '' TCONTAB, --PENDIENTE 06
                                                                COALESCE(DPR.CAPITAL,0) *
                                                                        CASE	WHEN	PRE.BRANCH = 66
                                                                                        THEN	(	SELECT	MAX(EXC.EXCHANGE)
                                                                                                                FROM 	USINSUG01.EXCHANGE EXC
                                                                                                                WHERE	EXC.USERCOMP = PRE.USERCOMP 
                                                                                                                AND 	EXC.COMPANY = PRE.COMPANY 
                                                                                                                AND 	EXC.CURRENCY = 99
                                                                                                                AND 	EXC.EFFECDATE <= PRE.EFFECDATE
                                                                                                                AND 	(EXC.NULLDATE IS NULL OR EXC.NULLDATE > PRE.EFFECDATE))
                                                                                        ELSE	1 END VCAPITRC,
                                                                '' VCAPRCMA, --EXCLUIDO
                                                                '' VCAPRCCO, --EXCLUIDO
                                                                '' VMTSELO, --DESCARTADO
                                                                '' VMTFGAOU, --DESCARTADO
                                                                '' VMTFGARC, --DESCARTADO
                                                                '' KEBENTID_CH, --EXCLUIDO
                                                                '' DINDDESD, --EXCLUIDO
                                                                '' DARQUIVO, --EXCLUIDO
                                                                '' TARQUIVO, --EXCLUIDO
                                                                '' KRCINDREG, --EXCLUIDO
                                                                '' KRCINDORIG, --EXCLUIDO
                                                                '' KRCSUBES, --EXCLUIDO
                                                                '' KRCINDGBAN, --EXCLUIDO
                                                                '' DDESTPPROC, --EXCLUIDO
                                                                '' DCASHBACK --EXCLUIDO 
                                                from	(select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                then pre.ctid
                                                                                else null end pre_id,
                                                                dpr.certif,
                                                                sum(case 	when 	exists
                                                                                                        (	select	 1
                                                                                                                from	usinsug01.gen_cover gco
                                                                                                                join	usinsug01.tab_gencov tgc
                                                                                                                                on		tgc.usercomp = gco.usercomp 
                                                                                                                                and 	tgc.company = gco.company 
                                                                                                                                and 	tgc.currency = gco.currency
                                                                                                                                and 	tgc.cover = gco.covergen
                                                                                                                                and 	lower(tgc.descript) like 'r%civil%'
                                                                                                                where	gco.usercomp = pre.usercomp
                                                                                                                and		gco.company = pre.company
                                                                                                                and		gco.branch = pre.branch
                                                                                                                and		gco.product = pre.product
                                                                                                                and		gco.currency = pre.currency
                                                                                                                and		gco.cover = dpr.code)
                                                                                        then 	coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end
                                                                                        else 	0 end) capital,
                                                                SUM(case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                        then coalesce(dpr.premium,0)
                                                                                        else 0 end) premium, --prima contable
                                                                SUM(case	when dpr.type_detai in ('3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                        then coalesce(dpr.premium,0)
                                                                                        else 0 end) premium_recdes, --de la prima contable, los recargos y descuentos
                                                                SUM(case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                        then coalesce(dpr.commision,0)
                                                                                        else 0 end) commision --comisión
                                                from	usinsug01.premium pre
                                                join	usinsug01.detail_pre dpr
                                                                on		dpr.usercomp = pre.usercomp
                                                                and 	dpr.company = pre.company
                                                                and 	dpr.receipt = pre.receipt
                                                where 	pre.usercomp = 1
                                                and 	pre.company = 1
                                                and 	pre.branch <> 1 --excluyendo incendio, probar con otros ramos
                                                and     pre.branch between  3 and 5 --Luego quitar este filtro
                                                and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                and 	pre.statusva not in ('2','3') 
                                                group 	by 1,2) dpr
                                                join	usinsug01.premium pre on pre.ctid = dpr.pre_id    
                                           ) AS TMP    
                                           '''

    DF_LPG_INSUNIX_OTROS_RAMOS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECPR_INSUNIX_LPG_OTROS_RAMOS).load()


    L_RBRECPR_INSUNIX_LPG_RAMO_INCENDIO = f'''
                                          (
                                            select 'D' as INDDETREC,
		                            'RBRECPR' as TABLAIFRS17,
		                            '' PK,
		                            '' DTPREG, --excluido
		                            '' TIOCPROC, --excluido
		                            '' TIOCFRM, --excluido
		                            '' TIOCTO, --excluido
		                            '' KRITPREG, --PENDIENTE 01
		                            'PIG' KGIORIGM, --excluido
		                            pre.branch KGCRAMO,
		                            pre.policy DNUMAPO,
		                            coalesce(dpr.certif,0) DNMCERT,
		                            pre.receipt DNUMREC,
		                            '' DNMAGRE, --PENDIENTE 02
		                            '' NSAGREG, --excluido
		                            '' KEBENTID, --excluido
		                            pre.issuedat TEMISSAO, 
		                            pre.effecdate TINICIO,
		                            pre.expirdat TTERMO,
		                            pre.statdate TESTADO,
		                            pre.factdate TLIMCOB,
		                            case 	when	pre.status_pre = '2'
		                            		then	(	select	max(statdate)
		                            					from	usinsug01.premium_mo
		                            					where	usercomp = pre.usercomp
		                            					and		company = pre.company
		                            					and		receipt = pre.receipt
		                            					and		type = 2)
		                            		else 	null end TPGCOB, --SIN PERMISOS EN PROD. usinsug01.premium_mo (1)
		                            case 	when	pre.status_pre = '3'
		                            		then	(	select	max(statdate)
		                            					from	usinsug01.premium_mo
		                            					where	usercomp = pre.usercomp
		                            					and		company = pre.company
		                            					and		receipt = pre.receipt
		                            					and		type = '7')
		                            		else 	null end TANSUSP, --SIN PERMISOS EN PROD. usinsug01.premium_mo (2)
		                            pre.factdate + 1 TDEVIDO,
		                            pre.currency KRCMOEDA,
		                            '' VCAMBIO, --descartado
		                            dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
		                            	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            					from	usinsug01.detail_pre dp0
		                            					where	dp0.usercomp = pre.usercomp
		                            					and 	dp0.company = pre.company
		                            					and 	dp0.receipt = pre.receipt
		                            					and		dp0.certif = dpr.certif
		                            					and		not (dp0.type_detai in ('1','3','4') and dp0.bill_item not in (4,5,9,97))
		                            					and		dp0.bill_item <> 9),0) --se excluye el igv --prima otros (nivel_1b)
		                            		--distribución de nivel_1b por coberturas, es decir nivel_1a + nivel_1b* (dn1) *esta prima no se abre por conceptos como nivel_1a
		                            		*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1a es 0 - caso regular/atípico)
		                            					and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima contable - caso atípico)
		                            						(	select	1
		                            							from	usinsug01.detail_pre dp0
		                            							where	dp0.usercomp = pre.usercomp
		                            							and		dp0.company  = pre.company
		                            							and		dp0.receipt = pre.receipt
		                            							and		dp0.certif = dpr.certif
		                            							and 	dp0.type_detai in ('1','3','4')
		                            							and		dp0.bill_item not in (4,5,9,97)
		                            							and		coalesce(dp0.premium,0) <> 0)
		                            					then	coalesce(
		                            							(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            												from	usinsug01.detail_pre dp0
		                            												where	dp0.usercomp = pre.usercomp
		                            												and		dp0.company  = pre.company
		                            												and		dp0.receipt = pre.receipt
		                            												and		dp0.certif = dpr.certif
		                            												and 	dp0.type_detai in ('1','3','4')
		                            												and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            								else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            						--** de no cumplir dn1, nivel_1b se distribuye por recibo/certificado/coberturas (evita perderse nivel_1b por nivel_1b = 0)
		                            					end) +
		                            	--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            	--(si el certificado es 0, ya fue calculado en nivel_1b)
		                            	case	when	coalesce(dpr.certif,0) <> 0
		                            			then	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            								from	usinsug01.detail_pre dp0
		                            								where	dp0.usercomp = pre.usercomp
		                            								and 	dp0.company = pre.company
		                            								and 	dp0.receipt = pre.receipt
		                            								and		dp0.certif = 0
		                            								and		dp0.bill_item <> 9),0) --se excluye el igv 
		                            							*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
		                            			else 	0 end VMTCOMR,
		                            (	dpr.premium_recdes + --prima contable a nivel del certificado/cobertura (nivel_1a)
		                            		--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            		--(si el certificado es 0, ya fue calculado en nivel_1b)
		                            		case	when	coalesce(dpr.certif,0) <> 0
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
		                            	((	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            					from	usinsug01.detail_pre dp0
		                            					where	dp0.usercomp = pre.usercomp
		                            					and 	dp0.company = pre.company
		                            					and 	dp0.receipt = pre.receipt
		                            					and		dp0.certif = dpr.certif
		                            					and		dp0.bill_item = 5),0) --prima der. emisión (nivel_1)
		                            		--distribución de nivel_1 por coberturas (dn1) * esta prima no se abre por conceptos, así que debe de distribuirse por coberturas
		                            		*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1 es 0 - no se debe de aplicar distribución o "infla" montos)
		                            					and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima, para evitar "inflar" montos)
		                            						(	select	1
		                            							from	usinsug01.detail_pre dp0
		                            							where	dp0.usercomp = pre.usercomp
		                            							and		dp0.company  = pre.company
		                            							and		dp0.receipt = pre.receipt
		                            							and		dp0.certif = dpr.certif
		                            							and 	dp0.type_detai in ('1','3','4')
		                            							and		dp0.bill_item not in (4,5,9,97)
		                            							and		coalesce(dp0.premium,0) <> 0)
		                            					then	coalesce(
		                            							(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            												from	usinsug01.detail_pre dp0
		                            												where	dp0.usercomp = pre.usercomp
		                            												and		dp0.company  = pre.company
		                            												and		dp0.receipt = pre.receipt
		                            												and		dp0.certif = dpr.certif
		                            												and 	dp0.type_detai in ('1','3','4')
		                            												and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            								else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            							--** de no cumplir dn1, nivel_1 se distribuye por recibo/certificado/coberturas (evita perderse por nivel_1 = 0)
		                            					end) +
		                            		--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            		--(si el certificado es 0, ya fue calculado en nivel_1)
		                            		case	when	coalesce(dpr.certif,0) <> 0
		                            				then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            										from	usinsug01.detail_pre dp0
		                            										where	dp0.usercomp = pre.usercomp
		                            										and 	dp0.company = pre.company
		                            										and 	dp0.receipt = pre.receipt
		                            										and		dp0.certif = 0
		                            										and		dp0.bill_item = 5),0) --solo interesa los casos con el monto asociado al der. emisión
		                            							*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            				else 	0 end) VMTENCG,
		                            (	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            				from	usinsug01.detail_pre dp0
		                            				where	dp0.usercomp = pre.usercomp
		                            				and 	dp0.company = pre.company
		                            				and 	dp0.receipt = pre.receipt
		                            				and		dp0.certif = dpr.certif
		                            				and		(dp0.type_detai = '2' or dp0.bill_item = 9)),0) --impuestos('2') e igv (9) (nivel_1)
		                            	--distribución de nivel_1 por coberturas (dn1) * esta prima no se abre por conceptos, así que debe de distribuirse por coberturas
		                            	*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1 es 0 - no se debe de aplicar distribución o "infla" montos)
		                            				and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima, para evitar "inflar" montos)
		                            					(	select	1
		                            						from	usinsug01.detail_pre dp0
		                            						where	dp0.usercomp = pre.usercomp
		                            						and		dp0.company  = pre.company
		                            						and		dp0.receipt = pre.receipt
		                            						and		dp0.certif = dpr.certif
		                            						and 	dp0.type_detai in ('1','3','4')
		                            						and		dp0.bill_item not in (4,5,9,97)
		                            						and		coalesce(dp0.premium,0) <> 0)
		                            			then	coalesce(
		                            					(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            										from	usinsug01.detail_pre dp0
		                            										where	dp0.usercomp = pre.usercomp
		                            										and		dp0.company  = pre.company
		                            										and		dp0.receipt = pre.receipt
		                            										and		dp0.certif = dpr.certif
		                            										and 	dp0.type_detai in ('1','3','4')
		                            										and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            						else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            					--** de no cumplir dn1, nivel_1 se distribuye por recibo/certificado/coberturas (evita perderse por nivel_1 = 0)
		                            				end) +
		                            	--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            	--(si el certificado es 0, ya fue calculado en nivel_1)
		                            	case	when	coalesce(dpr.certif,0) <> 0
		                            			then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            									from	usinsug01.detail_pre dp0
		                            									where	dp0.usercomp = pre.usercomp
		                            									and 	dp0.company = pre.company
		                            									and 	dp0.receipt = pre.receipt
		                            									and		dp0.certif = 0
		                            									and		(dp0.type_detai = '2' or dp0.bill_item = 9)),0) --solo interesa los casos con el monto asociado al impuesto e igv
		                            						*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            			else 	0 end VMTIMPO,
		                            '' VMTPRMPR, --descartado
		                            '' VMTPRMBR, --descartado
		                            '' VMTPRMTR, --excluido
		                            '' VMTPRMAB, --descartado
		                            '' VMTJURO, --PENDIENTE 03
		                            '' VMTBONU, --excluido
		                            '' VMTDESC, --excluido
		                            '' VMTAGRA, --excluido
		                            dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
		                            	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            					from	usinsug01.detail_pre dp0
		                            					where	dp0.usercomp = pre.usercomp
		                            					and 	dp0.company = pre.company
		                            					and 	dp0.receipt = pre.receipt
		                            					and		dp0.certif = dpr.certif
		                            					and		not (dp0.type_detai in ('1','3','4') 
		                            							and dp0.bill_item not in (4,5,9,97))),0) --prima otros (nivel_1b)
		                            		--distribución de nivel_1b por coberturas, es decir nivel_1a + nivel_1b* (dn1) *esta prima no se abre por conceptos como nivel_1a
		                            		*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1a es 0 - caso regular/atípico)
		                            					and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima contable - caso atípico)
		                            						(	select	1
		                            							from	usinsug01.detail_pre dp0
		                            							where	dp0.usercomp = pre.usercomp
		                            							and		dp0.company  = pre.company
		                            							and		dp0.receipt = pre.receipt
		                            							and		dp0.certif = dpr.certif
		                            							and 	dp0.type_detai in ('1','3','4')
		                            							and		dp0.bill_item not in (4,5,9,97)
		                            							and		coalesce(dp0.premium,0) <> 0)
		                            					then	coalesce(
		                            							(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            												from	usinsug01.detail_pre dp0
		                            												where	dp0.usercomp = pre.usercomp
		                            												and		dp0.company  = pre.company
		                            												and		dp0.receipt = pre.receipt
		                            												and		dp0.certif = dpr.certif
		                            												and 	dp0.type_detai in ('1','3','4')
		                            												and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            					else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            						--** de no cumplir dn1, nivel_1b se distribuye por recibo/certificado/coberturas (evita perderse nivel_1b por nivel_1b = 0)
		                            					end) +
		                            	--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            	--(si el certificado es 0, ya fue calculado en nivel_1b)
		                            	case	when	coalesce(dpr.certif,0) <> 0
		                            			then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            									from	usinsug01.detail_pre dp0
		                            									where	dp0.usercomp = pre.usercomp
		                            									and 	dp0.company = pre.company
		                            									and 	dp0.receipt = pre.receipt
		                            									and		dp0.certif = 0),0)
		                            						*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            			else 	0 end VMTTOTRP,
		                            '' VCAPITAL, --excluido
		                            '' KRCFMPGP, --descartado
		                            pre.status_pre KRCESTRP,
		                            '' KRCMOSTP, --excluido
		                            pre.tratypei KRCTPRCP,
		                            '' KCBMEDCB, --excluido
		                            '' KCBMEDCE, --excluido
		                            '' KCBMEDP2, --excluido
		                            '' KCBMEDRA, --excluido
		                            '' KRCESPRP, --excluido
		                            '' KRCTPCSG, --excluido
		                            '' KCBMEDPD, --excluido
		                            pre.branch || '-' || pre.product || '- 0' KABPRODT,
		                            '' KRCTPFRC, --PENDIENTE 04
		                            (	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            				from	usinsug01.detail_pre dp0
		                            				where	dp0.usercomp = pre.usercomp
		                            				and 	dp0.company = pre.company
		                            				and 	dp0.receipt = pre.receipt
		                            				and		dp0.certif = dpr.certif
		                            				and		dp0.bill_item = 4),0) --prima rgc (nivel_1)
		                            	--distribución de nivel_1 por coberturas (dn1) * esta prima no se abre por conceptos, así que debe de distribuirse por coberturas
		                            	*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1 es 0 - no se debe de aplicar distribución o "infla" montos)
		                            				and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima, para evitar "inflar" montos)
		                            					(	select	1
		                            						from	usinsug01.detail_pre dp0
		                            						where	dp0.usercomp = pre.usercomp
		                            						and		dp0.company  = pre.company
		                            						and		dp0.receipt = pre.receipt
		                            						and		dp0.certif = dpr.certif
		                            						and 	dp0.type_detai in ('1','3','4')
		                            						and		dp0.bill_item not in (4,5,9,97)
		                            						and		coalesce(dp0.premium,0) <> 0)
		                            				then	coalesce(
		                            						(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            											from	usinsug01.detail_pre dp0
		                            											where	dp0.usercomp = pre.usercomp
		                            											and		dp0.company  = pre.company
		                            											and		dp0.receipt = pre.receipt
		                            											and		dp0.certif = dpr.certif
		                            											and 	dp0.type_detai in ('1','3','4')
		                            											and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            						else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            					--** de no cumplir dn1, nivel_1 se distribuye por recibo/certificado/coberturas (evita perderse por nivel_1 = 0)
		                            				end) +
		                            	--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            	--(si el certificado es 0, ya fue calculado en nivel_1)
		                            	case	when	coalesce(dpr.certif,0) <> 0
		                            			then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            									from	usinsug01.detail_pre dp0
		                            									where	dp0.usercomp = pre.usercomp
		                            									and 	dp0.company = pre.company
		                            									and 	dp0.receipt = pre.receipt
		                            									and		dp0.certif = 0
		                            									and		dp0.bill_item = 4),0) --solo interesa los casos con el monto asociado al concepto rgc
		                            						*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            			else 	0 end VMTCOMCB,
		                            dpr.commision + --existen registros de primas con comisión
		                            	case	when	coalesce(dpr.certif,0) <> 0
		                            			then	(	coalesce((	select	sum(coalesce(dp0.commision,0))
		                            									from	usinsug01.detail_pre dp0
		                            									where	dp0.usercomp = pre.usercomp
		                            									and 	dp0.company = pre.company
		                            									and 	dp0.receipt = pre.receipt
		                            									and		dp0.certif = 0
		                            									and		dp0.type_detai in ('1','3','4')
		                            									and		dp0.bill_item not in (4,5,9,97)),0) *
		                            						(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            			else 	0 end VMTCOMMD,
		                            '' VMTCOMME, --excluido
		                            '' VMTCSAP, --excluido
		                            '' VMTCSCV, --descartado
		                            (	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            				from	usinsug01.detail_pre dp0
		                            				where	dp0.usercomp = pre.usercomp
		                            				and 	dp0.company = pre.company
		                            				and 	dp0.receipt = pre.receipt
		                            				and		dp0.certif = dpr.certif
		                            				and		dp0.bill_item = 97),0) --prima g. financ. (nivel_1)
		                            	--distribución de nivel_1 por coberturas (dn1) * esta prima no se abre por conceptos, así que debe de distribuirse por coberturas
		                            	*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1 es 0 - no se debe de aplicar distribución o "infla" montos)
		                            				and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima, para evitar "inflar" montos)
		                            					(	select	1
		                            						from	usinsug01.detail_pre dp0
		                            						where	dp0.usercomp = pre.usercomp
		                            						and		dp0.company  = pre.company
		                            						and		dp0.receipt = pre.receipt
		                            						and		dp0.certif = dpr.certif
		                            						and 	dp0.type_detai in ('1','3','4')
		                            						and		dp0.bill_item not in (4,5,9,97)
		                            						and		coalesce(dp0.premium,0) <> 0)
		                            				then	coalesce(
		                            						(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            											from	usinsug01.detail_pre dp0
		                            											where	dp0.usercomp = pre.usercomp
		                            											and		dp0.company  = pre.company
		                            											and		dp0.receipt = pre.receipt
		                            											and		dp0.certif = dpr.certif
		                            											and 	dp0.type_detai in ('1','3','4')
		                            											and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            							else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            					--** de no cumplir dn1, nivel_1 se distribuye por recibo/certificado/coberturas (evita perderse por nivel_1 = 0)
		                            				end) +
		                            	--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            	--(si el certificado es 0, ya fue calculado en nivel_1)
		                            	case	when	coalesce(dpr.certif,0) <> 0
		                            			then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            									from	usinsug01.detail_pre dp0
		                            									where	dp0.usercomp = pre.usercomp
		                            									and 	dp0.company = pre.company
		                            									and 	dp0.receipt = pre.receipt
		                            									and		dp0.certif = 0
		                            									and		dp0.bill_item = 97),0) --solo interesa los casos con el monto asociado al der. emisión
		                            						*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            						else 	0 end VMTCSFR,
		                            '' VMTIMPSL, --descartado
		                            '' VMTFAT, --descartado
		                            '' VMTFGA, --descartado
		                            '' VMTSNB, --descartado
		                            '' VMTINEM, --descartado
		                            '' VMTFUCA, --descartado
		                            (	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
		                            		(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            						from	usinsug01.detail_pre dp0
		                            						where	dp0.usercomp = pre.usercomp
		                            						and 	dp0.company = pre.company
		                            						and 	dp0.receipt = pre.receipt
		                            						and		dp0.certif = dpr.certif
		                            						and		not (dp0.type_detai in ('1','3','4') 
		                            								and dp0.bill_item not in (4,5,9,97))),0) --prima otros (nivel_1b)
		                            			--distribución de nivel_1b por coberturas, es decir nivel_1a + nivel_1b* (dn1) *esta prima no se abre por conceptos como nivel_1a
		                            			*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1a es 0 - caso regular/atípico)
		                            						and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima contable - caso atípico)
		                            							(	select	1
		                            								from	usinsug01.detail_pre dp0
		                            								where	dp0.usercomp = pre.usercomp
		                            								and		dp0.company  = pre.company
		                            								and		dp0.receipt = pre.receipt
		                            								and		dp0.certif = dpr.certif
		                            								and 	dp0.type_detai in ('1','3','4')
		                            								and		dp0.bill_item not in (4,5,9,97)
		                            								and		coalesce(dp0.premium,0) <> 0)
		                            						then	coalesce(
		                            								(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            													from	usinsug01.detail_pre dp0
		                            													where	dp0.usercomp = pre.usercomp
		                            													and		dp0.company  = pre.company
		                            													and		dp0.receipt = pre.receipt
		                            													and		dp0.certif = dpr.certif
		                            													and 	dp0.type_detai in ('1','3','4')
		                            													and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            						else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            							--** de no cumplir dn1, nivel_1b se distribuye por recibo/certificado/coberturas (evita perderse nivel_1b por nivel_1b = 0)
		                            						end) +
		                            		--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            		--(si el certificado es 0, ya fue calculado en nivel_1b)
		                            		case	when	coalesce(dpr.certif,0) <> 0
		                            				then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            										from	usinsug01.detail_pre dp0
		                            										where	dp0.usercomp = pre.usercomp
		                            										and 	dp0.company = pre.company
		                            										and 	dp0.receipt = pre.receipt
		                            										and		dp0.certif = 0),0)
		                            							*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            				else 	0 end)  *
		                            		(coalesce((	select	coalesce(coi.share,0)
		                            					from	usinsug01.coinsuran coi
		                            					where	coi.usercomp = pre.usercomp
		                            					and     coi.company = pre.company
		                            					and     coi.certype = pre.certype
		                            					and     coi.branch = pre.branch
		                            					and     coi.policy = pre.policy
		                            					and     coi.effecdate <= pre.effecdate
		                            					and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
		                            					and 	coalesce(coi.companyc,0) = 1),100) / 100) VMTCOSEG, --cálculo coaseguro retenido
		                            ((	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            				from	usinsug01.detail_pre dp0
		                            				where	dp0.usercomp = pre.usercomp
		                            				and 	dp0.company = pre.company
		                            				and 	dp0.receipt = pre.receipt
		                            				and		dp0.certif = dpr.certif
		                            				and		dp0.bill_item = 97),0) --prima g. financ. (nivel_1)
		                            	--distribución de nivel_1 por coberturas (dn1) * esta prima no se abre por conceptos, así que debe de distribuirse por coberturas
		                            	*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1 es 0 - no se debe de aplicar distribución o "infla" montos)
		                            				and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima, para evitar "inflar" montos)
		                            					(	select	1
		                            						from	usinsug01.detail_pre dp0
		                            						where	dp0.usercomp = pre.usercomp
		                            						and		dp0.company  = pre.company
		                            						and		dp0.receipt = pre.receipt
		                            						and		dp0.certif = dpr.certif
		                            						and 	dp0.type_detai in ('1','3','4')
		                            						and		dp0.bill_item not in (4,5,9,97)
		                            						and		coalesce(dp0.premium,0) <> 0)
		                            				then	coalesce(
		                            						(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            											from	usinsug01.detail_pre dp0
		                            											where	dp0.usercomp = pre.usercomp
		                            											and		dp0.company  = pre.company
		                            											and		dp0.receipt = pre.receipt
		                            											and		dp0.certif = dpr.certif
		                            											and 	dp0.type_detai in ('1','3','4')
		                            											and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            							else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            					--** de no cumplir dn1, nivel_1 se distribuye por recibo/certificado/coberturas (evita perderse por nivel_1 = 0)
		                            				end) +
		                            	--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            	--(si el certificado es 0, ya fue calculado en nivel_1)
		                            	case	when	coalesce(dpr.certif,0) <> 0
		                            			then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            									from	usinsug01.detail_pre dp0
		                            									where	dp0.usercomp = pre.usercomp
		                            									and 	dp0.company = pre.company
		                            									and 	dp0.receipt = pre.receipt
		                            									and		dp0.certif = 0
		                            									and		dp0.bill_item = 97),0) --solo interesa los casos con el monto asociado al der. emisión
		                            						*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            						else 	0 end)  *
		                            		(coalesce((	select	coalesce(coi.share,0)
		                            					from	usinsug01.coinsuran coi
		                            					where	coi.usercomp = pre.usercomp
		                            					and     coi.company = pre.company
		                            					and     coi.certype = pre.certype
		                            					and     coi.branch = pre.branch
		                            					and     coi.policy = pre.policy
		                            					and     coi.effecdate <= pre.effecdate
		                            					and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
		                            					and 	coalesce(coi.companyc,0) = 1),100) / 100) VMTCSTFC,
		                            (	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
		                            		(	(	select	sum(coalesce(dp0.premium,0))
		                            				from	usinsug01.detail_pre dp0
		                            				where	dp0.usercomp = pre.usercomp
		                            				and 	dp0.company = pre.company
		                            				and 	dp0.receipt = pre.receipt
		                            				and		dp0.certif = dpr.certif
		                            				and		not (dp0.type_detai in ('1','3','4') 
		                            						and dp0.bill_item not in (4,5,9,97))) --prima otros (nivel_1b)
		                            			--distribución de nivel_1b por coberturas, es decir nivel_1a + nivel_1b* (dn1) *esta prima no se abre por conceptos como nivel_1a
		                            			*	case	when dpr.premium = 0 --prevalidación dn1a (cuando nivel_1a es 0 - caso regular/atípico)
		                            						and	not exists --prevalidación dn1b (no existe o es 0 en otras coberturas del certificado la prima contable - caso atípico)
		                            							(	select	1
		                            								from	usinsug01.detail_pre dp0
		                            								where	dp0.usercomp = pre.usercomp
		                            								and		dp0.company  = pre.company
		                            								and		dp0.receipt = pre.receipt
		                            								and		dp0.certif = dpr.certif
		                            								and 	dp0.type_detai in ('1','3','4')
		                            								and		dp0.bill_item not in (4,5,9,97)
		                            								and		coalesce(dp0.premium,0) <> 0)
		                            						then	coalesce(
		                            								(1 /	nullif((	select	cast(count(distinct dp0.bill_item) as decimal(20)) --de cumplirse dn1, se distribuye la prima por x coberturas
		                            													from	usinsug01.detail_pre dp0
		                            													where	dp0.usercomp = pre.usercomp
		                            													and		dp0.company  = pre.company
		                            													and		dp0.receipt = pre.receipt
		                            													and		dp0.certif = dpr.certif
		                            													and 	dp0.type_detai in ('1','3','4')
		                            													and		dp0.bill_item not in (4,5,9,97)),0)),0)
		                            						else	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id, dpr.certif),0),0)) --**
		                            							--** de no cumplir dn1, nivel_1b se distribuye por recibo/certificado/coberturas (evita perderse nivel_1b por nivel_1b = 0)
		                            						end) +
		                            		--se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
		                            		--(si el certificado es 0, ya fue calculado en nivel_1b)
		                            		case	when	coalesce(dpr.certif,0) <> 0
		                            				then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
		                            										from	usinsug01.detail_pre dp0
		                            										where	dp0.usercomp = pre.usercomp
		                            										and 	dp0.company = pre.company
		                            										and 	dp0.receipt = pre.receipt
		                            										and		dp0.certif = 0),0)
		                            							*	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
		                            				else 	0 end) *
		                            		(coalesce((	select	coalesce(coi.share,0)
		                            					from	usinsug01.coinsuran coi
		                            					where	coi.usercomp = pre.usercomp
		                            					and     coi.company = pre.company
		                            					and     coi.certype = pre.certype
		                            					and     coi.branch = pre.branch
		                            					and     coi.policy = pre.policy
		                            					and     coi.effecdate <= pre.effecdate
		                            					and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
		                            					and 	coalesce(coi.companyc,0) = 1),100) / 100) * --cálculo coaseguro retenido
		                            		(	coalesce((	select  sum(coalesce(rei.share,0)) --cálculo reaseguro retenido
		                            						from    usinsug01.reinsuran rei
		                            						where   rei.usercomp = pre.usercomp
		                            						and     rei.company = pre.company
		                            						and     rei.certype = pre.certype
		                            						and     rei.branch = pre.branch
		                            						and     rei.policy = pre.policy
		                            						and     rei.certif = coalesce(dpr.certif,0)
		                            						and     rei.effecdate <= pre.effecdate
		                            						and     (rei.nulldate is null or rei.nulldate > pre.effecdate)
		                            						and     coalesce(rei.type,0) = 1),100) / 100) VMTRESSG,
		                            '' VMTCMCCS, --descartado
		                            '' DUSRUPD, --excluido
		                            '' VMTCOMFD, --descartado
		                            '' VMTCOMPG, --descartado
		                            'LPG' DCOMPA,
		                            '' DMARCA, --excluido
		                            '' KRBRECPR_MP, --excluido
		                            '' TMIGPARA, --excluido
		                            '' KRBRECPR_MD, --excluido
		                            '' TMIGDE, --excluido
		                            pre.branch || '-' || pre.policy || '-' || coalesce(dpr.certif,0) KABAPOL,
		                            '' KABAPOL_EFT, --excluido
		                            '' KABAPOL_GRP, --excluido
		                            '' DNUMPRES, --PENDIENTE 05
		                            coalesce((	select 	case	when	pre.branch = 1
		                            							then	acc.branch_pyg
		                            							else	acc.branch_bal end
		                            			from	usinsug01.acc_autom2 acc
		                            			where	ctid =
		                            					coalesce(
		                            		                (   select  min(ctid) --búsqueda normal en usinsug01.acc_autom2(ramo;producto;concepto 1 para todos, excepto incendio)
		                            		                    from    usinsug01.acc_autom2 abe
		                            		                    where   abe.branch = pre.branch
		                            		                    and 	abe.product = pre.product
		                            		                    and 	abe.concept_fac =
		                            		                    		case	when	pre.branch <> 1
		                            		                    				then 	1 --es universal para todos los casos, excepto incendio
		                            		                    				else 	coalesce(dpr.bill_item,1) --en caso por error esté sin valor se asigna el base
		                            		                    				end),
		                            		                (   select  min(ctid) --búsqueda igual a anterior, pero en caso el producto no exista en usinsug01.acc_autom2 (error LP)
		                            		                    from    usinsug01.acc_autom2 abe
		                            		                    where   abe.branch = pre.branch
		                            		                    and 	abe.concept_fac =
		                            		                    		case	when	pre.branch <> 1
		                            		                    				then 	1 --es universal para todos los casos, excepto incendio
		                            		                    				else 	coalesce(dpr.bill_item,1) --en caso por error esté sin valor se asigna el base
		                            		                    				end))),0) KGCRAMO_SAP,
		                            '' KRCRGMDL, --excluido
		                            '' DNIB, --excluido
		                            '' DREFEATM, --excluido
		                            '' DINDRESG, --excluido
		                            '' TINICIOA, --excluido
		                            '' TTERMOA, --excluido
		                            '' DTERMO, --excluido
		                            '' DDEUNRIS, --excluido
		                            '' TCONTAB, --PENDIENTE 06
		                            coalesce(dpr.capital,0) VCAPITRC,
		                            '' VCAPRCMA, --excluido
		                            '' VCAPRCCO, --excluido
		                            '' VMTSELO, --descartado
		                            '' VMTFGAOU, --descartado
		                            '' VMTFGARC, --descartado
		                            '' KEBENTID_CH, --excluido
		                            '' DINDDESD, --excluido
		                            '' DARQUIVO, --excluido
		                            '' TARQUIVO, --excluido
		                            '' KRCINDREG, --excluido
		                            '' KRCINDORIG, --excluido
		                            '' KRCSUBES, --excluido
		                            '' KRCINDGBAN, --excluido
		                            '' DDESTPPROC, --excluido
		                            '' DCASHBACK --excluido
                                            from(	select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
							then pre.ctid
							else null end pre_id,
					                dpr.certif,
					                dpr.bill_item,
					                sum(case 	when 	exists
					                					(	select	1
					                						from	usinsug01.gen_cover gco
					                						join	usinsug01.tab_gencov tgc
					                								on		tgc.usercomp = gco.usercomp 
					                								and 	tgc.company = gco.company 
					                								and 	tgc.currency = gco.currency
					                								and 	tgc.cover = gco.covergen
					                								and 	lower(tgc.descript) like 'r%civil%'
					                						where	gco.usercomp = pre.usercomp
					                						and		gco.company = pre.company
					                						and		gco.branch = pre.branch
					                						and		gco.product = pre.product
					                						and		gco.currency = pre.currency
					                						and		gco.cover = dpr.code)
					                			then 	coalesce(dpr.capital,0) * case when addsuini = '1' then 1 else 0 end
					                			else 	0 end) capital,
					                SUM(case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
					                			then coalesce(dpr.premium,0)
					                			else 0 end) premium, --prima contable
					                SUM(case	when dpr.type_detai in ('3','4') and dpr.bill_item not in (4,5,9,97)
					                			then coalesce(dpr.premium,0)
					                			else 0 end) premium_recdes, --de la prima contable, los recargos y descuentos
					                SUM(case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
					                			then coalesce(dpr.commision,0)
					                			else 0 end) commision --comisión
			                                from	usinsug01.premium pre
			                                join	usinsug01.detail_pre dpr
			                                		on		dpr.usercomp = pre.usercomp
			                                		and 	dpr.company = pre.company
			                                		and 	dpr.receipt = pre.receipt
			                                where 	pre.usercomp = 1
			                                and 	pre.company = 1
			                                and		pre.branch = 1 --prueba incendio
			                                --and 	pre.receipt in (select receipt from premium where usercomp = 1 and company = 1 and branch = 1) --prueba por índice
			                                and 	cast(pre.effecdate as date) <= '12/31/2020'
			                                and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
			                                and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
			                                and 	pre.statusva not in ('2','3')
			                                group 	by 1,2,3) dpr
                                                                            join	usinsug01.premium pre on pre.ctid = dpr.pre_id
                                                                            --5m14s (ramo incendio)
                                          ) AS TMP
                                          '''

    DF_LPG_INSUNIX_RAMO_INCENDIO = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECPR_INSUNIX_LPG_RAMO_INCENDIO).load()  

    DF_LPG_INSUNIX =   DF_LPG_INSUNIX_OTROS_RAMOS.union(DF_LPG_INSUNIX_RAMO_INCENDIO)            

    L_RBRECPR_INSUNIX_LPV = f'''
                            (
                                select	'D' as INDDETREC,
                                        'RBRECPR' as TABLAIFRS17,
                                        '' PK,
                                        '' DTPREG, --excluido
                                        '' TIOCPROC, --excluido
                                        '' TIOCFRM, --excluido
                                        '' TIOCTO, --excluido
                                        '' KRITPREG, --PENDIENTE 01
                                        'PIV' KGIORIGM, --excluido
                                        pre.branch KGCRAMO,
                                        pre.policy DNUMAPO,
                                        coalesce(dpr.certif,0) DNMCERT,
                                        pre.receipt DNUMREC,
                                        '' DNMAGRE, --PENDIENTE 02
                                        '' NSAGREG, --excluido
                                        '' KEBENTID, --excluido
                                        pre.issuedat TEMISSAO, 
                                        pre.effecdate TINICIO,
                                        coalesce (cast (pre.expirdat as varchar),'') TTERMO,
                                        pre.statdate TESTADO,
                                        pre.factdate TLIMCOB,
                                        case 	when	pre.status_pre = '2'
                                                        then	(	select	max(statdate)
                                                                                from	usinsuv01.premium_mo
                                                                                where	usercomp = pre.usercomp
                                                                                and		company = pre.company
                                                                                and		receipt = pre.receipt
                                                                                and		type = 2)
                                                        else 	null end TPGCOB, --SIN PERMISOS EN PROD. PREMIUM_MO (1)
                                        case 	when	pre.status_pre = '3'
                                                        then	(	select	   coalesce (max(cast (statdate as varchar )),'')
                                                                                from	usinsuv01.premium_mo
                                                                                where	usercomp = pre.usercomp
                                                                                and		company = pre.company
                                                                                and		receipt = pre.receipt
                                                                                and		type = 7)
                                                        else 	'' end TANSUSP, --SIN PERMISOS EN PROD. PREMIUM_MO (2)
                                        pre.factdate + 1 TDEVIDO,
                                        pre.currency KRCMOEDA,
                                        '' VCAMBIO, --descartado
                                        dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                        from	usinsuv01.detail_pre dp0
                                                                        where	dp0.usercomp = pre.usercomp
                                                                        and 	dp0.company = pre.company
                                                                        and 	dp0.receipt = pre.receipt
                                                                        and		dp0.certif = dpr.certif
                                                                        and		not (dp0.type_detai in ('1','3','4') and dp0.bill_item not in (4,5,9,97))
                                                                        and		dp0.bill_item <> 9),0) + --se excluye el igv --prima otros (nivel_1b)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                then	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                        and 	dp0.company = pre.company
                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                        and		dp0.certif = 0
                                                                                                        and		dp0.bill_item <> 9),0) --se suma la ditribuci�n por la matriz en caso el certificado dpr no sea 0 
                                                                                                *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0))
                                                                else 	0 end VMTCOMR,
                                        (	dpr.premium_recdes + --prima recargos/descuentos a nivel del certificado/cobertura (nivel_1a)
                                                        --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                        --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                        case	when	coalesce(dpr.certif,0) <> 0
                                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                        and 	dp0.company = pre.company
                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                        and		dp0.certif = 0
                                                                                                                        and		dp0.type_detai in ('3','4') 
                                                                                                                        and		dp0.bill_item not in (4,5,9,97)),0)
                                                                                                *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                        else 	0 end) +
                                                (	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsuv01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = dpr.certif
                                                                                and		dp0.bill_item = 5),0) + --prima der. emisi�n (nivel_1)
                                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                                then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                                and 	dp0.company = pre.company
                                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                                and		dp0.certif = 0
                                                                                                                                and		dp0.bill_item = 5),0) --solo interesa los casos con el monto asociado al der. emisi�n
                                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                                else 	0 end) VMTENCG,
                                        coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                from	usinsuv01.detail_pre dp0
                                                                where	dp0.usercomp = pre.usercomp
                                                                and 	dp0.company = pre.company
                                                                and 	dp0.receipt = pre.receipt
                                                                and		dp0.certif = dpr.certif
                                                                and		(	(dp0.type_detai = '2' and dp0.type_detai in ('1','3','4') and dp0.bill_item not in (4,5,9,97)) 
                                                                                        or dp0.bill_item = 9)),0) +--impuestos('2') e igv (9) (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                and 	dp0.company = pre.company
                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                and		dp0.certif = 0
                                                                        and		(	(dp0.type_detai = '2' and dp0.type_detai in ('1','3','4') and dp0.bill_item not in (4,5,9,97)) 
                                                                                                or dp0.bill_item = 9)),0) --impuestos('2') e igv (9) (nivel_1)
                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                else 	0 end VMTIMPO,
                                        '' VMTPRMPR, --descartado
                                        '' VMTPRMBR, --descartado
                                        '' VMTPRMTR, --excluido
                                        '' VMTPRMAB, --descartado
                                        '' VMTJURO, --PENDIENTE 03
                                        '' VMTBONU, --excluido
                                        '' VMTDESC, --excluido
                                        '' VMTAGRA, --excluido
                                        dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                        from	usinsuv01.detail_pre dp0
                                                                        where	dp0.usercomp = pre.usercomp
                                                                        and 	dp0.company = pre.company
                                                                        and 	dp0.receipt = pre.receipt
                                                                        and		dp0.certif = dpr.certif
                                                                        and		not (dp0.type_detai in ('1','3','4') 
                                                                                        and dp0.bill_item not in (4,5,9,97))),0) + --prima otros (nivel_1b)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                and 	dp0.company = pre.company
                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                and		dp0.certif = 0),0)
                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                else 	0 end VMTTOTRP,
                                        '' VCAPITAL, --excluido
                                        '' KRCFMPGP, --descartado
                                        pre.status_pre KRCESTRP,
                                        '' KRCMOSTP, --excluido
                                        pre.tratypei KRCTPRCP,
                                        '' KCBMEDCB, --excluido
                                        '' KCBMEDCE, --excluido
                                        '' KCBMEDP2, --excluido
                                        '' KCBMEDRA, --excluido
                                        '' KRCESPRP, --excluido
                                        '' KRCTPCSG, --excluido
                                        '' KCBMEDPD, --excluido
                                        pre.branch || par.sep || pre.product KABPRODT,
                                        '' KRCTPFRC, --PENDIENTE 04
                                        coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                from	usinsuv01.detail_pre dp0
                                                                where	dp0.usercomp = pre.usercomp
                                                                and 	dp0.company = pre.company
                                                                and 	dp0.receipt = pre.receipt
                                                                and		dp0.certif = dpr.certif
                                                                and		dp0.bill_item = 4),0) + --prima rgc (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                and 	dp0.company = pre.company
                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                and		dp0.certif = 0
                                                                                                                and		dp0.bill_item = 4),0) --solo interesa los casos con el monto asociado al concepto rgc
                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                else 	0 end VMTCOMCB,
                                        dpr.commision + --existen registros de primas con comisi�n
                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                then	(	coalesce((	select	sum(coalesce(dp0.commision,0))
                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                and 	dp0.company = pre.company
                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                and		dp0.certif = 0
                                                                                                                and		dp0.type_detai in ('1','3','4')
                                                                                                                and		dp0.bill_item not in (4,5,9,97)),0) *
                                                                                        (coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                else 	0 end VMTCOMMD,
                                        '' VMTCOMME, --excluido
                                        '' VMTCSAP, --excluido
                                        '' VMTCSCV, --descartado
                                        coalesce((	select	sum(dp0.premium)
                                                                from	usinsuv01.detail_pre dp0
                                                                where	dp0.usercomp = pre.usercomp
                                                                and 	dp0.company = pre.company
                                                                and 	dp0.receipt = pre.receipt
                                                                and		dp0.certif = dpr.certif
                                                                and		dp0.bill_item = 97),0) + --prima g. financ. (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                and 	dp0.company = pre.company
                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                and		dp0.certif = 0
                                                                                                                and		dp0.bill_item = 97),0) --solo interesa los casos con el monto asociado al der. emisi�n
                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                                        else 	0 end VMTCSFR,
                                        '' VMTIMPSL, --descartado
                                        '' VMTFAT, --descartado
                                        '' VMTFGA, --descartado
                                        '' VMTSNB, --descartado
                                        '' VMTINEM, --descartado
                                        '' VMTFUCA, --descartado
                                        (	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                        coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsuv01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = dpr.certif
                                                                                and		not (dp0.type_detai in ('1','3','4') 
                                                                                                and dp0.bill_item not in (4,5,9,97))),0) + --prima otros (nivel_1b)
                                                        --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                        --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                        case	when	coalesce(dpr.certif,0) <> 0
                                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                        and 	dp0.company = pre.company
                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                        and		dp0.certif = 0),0)
                                                                                                *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                        else 	0 end) *
                                                        (coalesce(coi.share,100) / 100) VMTCOSEG, --c�lculo coaseguro retenido
                                        (	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                        from	usinsuv01.detail_pre dp0
                                                                        where	dp0.usercomp = pre.usercomp
                                                                        and 	dp0.company = pre.company
                                                                        and 	dp0.receipt = pre.receipt
                                                                        and		dp0.certif = dpr.certif
                                                                        and		dp0.bill_item = 97),0) + --prima g. financ. (nivel_1)
                                                --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                --(si el certificado es 0, ya fue calculado en nivel_1)
                                                case	when	coalesce(dpr.certif,0) <> 0
                                                                then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                from	usinsuv01.detail_pre dp0
                                                                                                                where	dp0.usercomp = pre.usercomp
                                                                                                                and 	dp0.company = pre.company
                                                                                                                and 	dp0.receipt = pre.receipt
                                                                                                                and		dp0.certif = 0
                                                                                                                and		dp0.bill_item = 97),0) --solo interesa los casos con el monto asociado al der. emisi�n
                                                                                        *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                                        else 	0 end) *
                                                (coalesce(coi.share,100) / 100) VMTCSTFC,
                                        (	dpr.premium + --prima contable a nivel del certificado/cobertura (nivel_1a)
                                                        coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                from	usinsuv01.detail_pre dp0
                                                                                where	dp0.usercomp = pre.usercomp
                                                                                and 	dp0.company = pre.company
                                                                                and 	dp0.receipt = pre.receipt
                                                                                and		dp0.certif = dpr.certif
                                                                                and		not (dp0.type_detai in ('1','3','4') 
                                                                                                and dp0.bill_item not in (4,5,9,97))),0) + --prima otros (nivel_1b)
                                                        --se procede a distribuir las primas en el certificado 0 si es que el certificado en dpr es diferente a 0
                                                        --(si el certificado es 0, ya fue calculado en nivel_1b)
                                                        case	when	coalesce(dpr.certif,0) <> 0
                                                                        then	(	coalesce((	select	sum(coalesce(dp0.premium,0))
                                                                                                                        from	usinsuv01.detail_pre dp0
                                                                                                                        where	dp0.usercomp = pre.usercomp
                                                                                                                        and 	dp0.company = pre.company
                                                                                                                        and 	dp0.receipt = pre.receipt
                                                                                                                        and		dp0.certif = 0),0)
                                                                                                *	(coalesce(dpr.premium / nullif(SUM(dpr.premium) OVER (partition by dpr.pre_id),0),0)))
                                                                        else 	0 end) *
                                                        (coalesce(coi.share,100) / 100) * 1 VMTRESSG,--c�lculo coaseguro retenido; aparte, no hay reaseguros para INX LPV
                                        '' VMTCMCCS, --descartado
                                        '' DUSRUPD, --excluido
                                        '' VMTCOMFD, --descartado
                                        '' VMTCOMPG, --descartado
                                        par.cia DCOMPA,
                                        '' DMARCA, --excluido
                                        '' KRBRECPR_MP, --excluido
                                        '' TMIGPARA, --excluido
                                        '' KRBRECPR_MD, --excluido
                                        '' TMIGDE, --excluido
                                        pre.branch || par.sep || pre.policy || par.sep || coalesce(dpr.certif,0) KABAPOL,
                                        '' KABAPOL_EFT, --excluido
                                        '' KABAPOL_GRP, --excluido
                                        '' DNUMPRES, --PENDIENTE 05
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
                                                                                                                                                else  (	select  min(sbs.cod_sbs_gyp)
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
                                                        else 	coalesce((	select  sbs.cod_sbs_gyp
                                                                                                from    usinsuv01.product_sbs tnb
                                                                                                join	usinsuv01.anexo1_sbs sbs
                                                                                                                on		sbs.cod_sbs_bal = tnb.cod_sbs_bal
                                                                                                                and     sbs.cod_sbs_gyp = tnb.cod_sbs_gyp
                                                                                                where   tnb.branch = pre.branch
                                                                                                and 	tnb.product = pre.product
                                                                                                and 	tnb.nulldate is null), 0)
                                                        end	KGCRAMO_SAP,
                                        '' KRCRGMDL, --excluido
                                        '' DNIB, --excluido
                                        '' DREFEATM, --excluido
                                        '' DINDRESG, --excluido
                                        '' TINICIOA, --excluido
                                        '' TTERMOA, --excluido
                                        '' DTERMO, --excluido
                                        '' DDEUNRIS, --excluido
                                        '' TCONTAB, --PENDIENTE 06
                                        0 VCAPITRC, --no aplica en LPV la cobertura RC
                                        '' VCAPRCMA, --excluido
                                        '' VCAPRCCO, --excluido
                                        '' VMTSELO, --descartado
                                        '' VMTFGAOU, --descartado
                                        '' VMTFGARC, --descartado
                                        '' KEBENTID_CH, --excluido
                                        '' DINDDESD, --excluido
                                        '' DARQUIVO, --excluido
                                        '' TARQUIVO, --excluido
                                        '' KRCINDREG, --excluido
                                        '' KRCINDORIG, --excluido
                                        '' KRCSUBES, --excluido
                                        '' KRCINDGBAN, --excluido
                                        '' DDESTPPROC, --excluido
                                        '' DCASHBACK --excluido 
                                        from	(	select	case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                then pre.ctid
                                                                                                else null end pre_id,
                                                                                dpr.certif,
                                                                                SUM(case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                        then coalesce(dpr.premium,0)
                                                                                                        else 0 end) premium, --prima contable
                                                                                SUM(case	when dpr.type_detai in ('3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                        then coalesce(dpr.premium,0)
                                                                                                        else 0 end) premium_recdes, --de la prima contable, los recargos y descuentos
                                                                                SUM(case	when dpr.type_detai in ('1','3','4') and dpr.bill_item not in (4,5,9,97)
                                                                                                        then coalesce(dpr.commision,0)
                                                                                                        else 0 end) commision --comisi�n
                                                                from	usinsuv01.premium pre
                                                                join	usinsuv01.detail_pre dpr
                                                                                on		dpr.usercomp = pre.usercomp
                                                                                and 	dpr.company = pre.company
                                                                                and 	dpr.receipt = pre.receipt
                                                                where 	pre.usercomp = 1
                                                                and 	pre.company = 1
                                                                and		pre.branch = 42 --prueba ramo 42
                                                                and 	cast(pre.effecdate as date) <= '12/31/2020'
                                                                and 	(pre.expirdat is null or cast(pre.expirdat as date) >= '12/31/2020')
                                                                and 	(pre.nulldate is null or cast(pre.nulldate as date) > '12/31/2020')
                                                                and 	pre.statusva not in ('2','3')
                                                                group 	by 1,2 limit 10) dpr
                                join	usinsuv01.premium pre on pre.ctid = dpr.pre_id
                                join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                left 	join usinsuv01.coinsuran coi --se valid� que saque la cantidad debida de registros con/sin el join a esta tabla (solo la retenci�n)
                                                on 		coi.usercomp = pre.usercomp
                                                and     coi.company = pre.company
                                                and     coi.certype = pre.certype
                                                and     coi.branch = pre.branch
                                                and     coi.policy = pre.policy
                                                and     coi.effecdate <= pre.effecdate
                                                and     (coi.nulldate is null or coi.nulldate > pre.effecdate)
                                                and 	coalesce(coi.companyc,0) = 1
                                --1m 19s
                            ) AS TMP
                            '''

    DF_LPV_INSUNIX = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECPR_INSUNIX_LPV).load()       
    
    L_DF_RBRECPR_INSUNIX = DF_LPG_INSUNIX.union(DF_LPV_INSUNIX)

    L_RBRECPR_VTIME_LPG = f'''
                              (
                                  select	'D' as INDDETREC,
                                                'RBRECPR' as TABLAIFRS17,
                                                '' PK, 
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --excluido
                                                '' TIOCFRM, --excluido
                                                '' TIOCTO, --excluido
                                                '' KRITPREG, --PENDIENTE 01
                                                'PVG' KGIORIGM, --excluido
                                                pre."NBRANCH" KGCRAMO,
                                                pre."NPOLICY" DNUMAPO,
                                                coalesce(prc.ncertif,coalesce(pre."NCERTIF",0)) DNMCERT,
                                                pre."NRECEIPT" DNUMREC,
                                                '' DNMAGRE, 
                                                '' NSAGREG, --excluido
                                                '' KEBENTID, --excluido
                                                cast(pre."DISSUEDAT" as date) TEMISSAO, 
                                                cast(pre."DEFFECDATE" as date) TINICIO,
                                                cast(pre."DEXPIRDAT" as date) TTERMO,
                                                cast(pre."DSTATDATE" as date) TESTADO,
                                                coalesce ( cast (cast(pre."DLIMITDATE" as date) as varchar),'') TLIMCOB,
                                                case 	when	pre."NSTATUS_PRE" = '2'
                                                                then	(	select	coalesce (cast (cast ( max("DSTATDATE") as date) as varchar),'')
                                                                                        from	usvtimg01."PREMIUM_MO"
                                                                                        where	"NRECEIPT" = pre."NRECEIPT"
                                                                                        and		"NDIGIT" = 0
                                                                                        and		"NTYPE" = 2)
                                                                else 	'' end TPGCOB, --SIN PERMISOS EN PROD. PREMIUM_MO (1)
                                                case 	when	pre."NSTATUS_PRE" = '3'
                                                                then	(	select	 coalesce (cast (cast( max("DSTATDATE") as date) as varchar),'')
                                                                                        from	usvtimg01."PREMIUM_MO"
                                                                                        where	"NRECEIPT" = pre."NRECEIPT"
                                                                                        and		"NDIGIT" = 0
                                                                                        and		"NTYPE" = 7)
                                                                else 	'' end TANSUSP, --SIN PERMISOS EN PROD. PREMIUM_MO (2)
                                                coalesce (cast (cast(pre."DLIMITDATE" as date) + 1 as varchar),'') TDEVIDO,
                                                pre."NCURRENCY" KRCMOEDA,
                                                '' VCAMBIO, --descartado
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_gfi) VMTCOMR,
                                                (prc.dpr_rec + prc.dpr_des + prc.dpr_der) VMTENCG,
                                                (prc.dpr_imp + prc.dpr_igv) VMTIMPO,
                                                '' VMTPRMPR, --descartado
                                                '' VMTPRMBR, --descartado
                                                '' VMTPRMTR, --excluido
                                                '' VMTPRMAB, --descartado
                                                '' VMTJURO, --PENDIENTE 03
                                                '' VMTBONU, --excluido
                                                '' VMTDESC, --excluido
                                                '' VMTAGRA, --excluido
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_imp + prc.dpr_gfi) VMTTOTRP,
                                                '' VCAPITAL, --excluido
                                                '' KRCFMPGP, --descartado
                                                pre."NSTATUS_PRE" KRCESTRP,
                                                '' KRCMOSTP, --excluido
                                                pre."NTRATYPEI" KRCTPRCP,
                                                '' KCBMEDCB, --excluido
                                                '' KCBMEDCE, --excluido
                                                '' KCBMEDP2, --excluido
                                                '' KCBMEDRA, --excluido
                                                '' KRCESPRP, --excluido
                                                '' KRCTPCSG, --excluido
                                                '' KCBMEDPD, --excluido
                                                pre."NBRANCH" || par.sep || pre."NPRODUCT" KABPRODT,
                                                '' KRCTPFRC, --PENDIENTE 04
                                                prc.dpr_rgc VMTCOMCB,
                                                prc.dpr_com VMTCOMMD, 
                                                '' VMTCOMME, --excluido
                                                '' VMTCSAP, --excluido
                                                '' VMTCSCV, --escartado
                                                prc.dpr_gfi VMTCSFR,
                                                '' VMTIMPSL, --descartado
                                                '' VMTFAT, --descartado
                                                '' VMTFGA, --descartado
                                                '' VMTSNB, --descartado
                                                '' VMTINEM, --descartado
                                                '' VMTFUCA, --descartado
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_igv + prc.dpr_gfi) * prc.ratio_coa VMTCOSEG, 
                                                prc.dpr_gfi * prc.ratio_coa VMTCSTFC,
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_igv + prc.dpr_gfi) * prc.ratio_coa * prc.ratio_rea VMTRESSG, 
                                                '' VMTCMCCS, --descartado
                                                '' DUSRUPD, --excluido
                                                '' VMTCOMFD, --descartado
                                                '' VMTCOMPG, --descartado
                                                par.cia DCOMPA,
                                                '' DMARCA, --excluido
                                                '' KRBRECPR_MP, --excluido
                                                '' TMIGPARA, --excluido
                                                '' KRBRECPR_MD, --excluido
                                                '' TMIGDE, --excluido
                                                pre."NBRANCH" || par.sep || pre."NPOLICY" || par.sep || coalesce(prc.ncertif,coalesce(pre."NCERTIF",0)) KABAPOL,
                                                '' KABAPOL_EFT, --excluido
                                                '' KABAPOL_GRP, --excluido
                                                '' DNUMPRES, --PENDIENTE 05
                                                prc.nbranch_led KGCRAMO_SAP,
                                                '' KRCRGMDL, --excluido
                                                '' DNIB, --excluido
                                                '' DREFEATM, --excluido
                                                '' DINDRESG, --excluido
                                                '' TINICIOA, --excluido
                                                '' TTERMOA, --excluido
                                                '' DTERMO, --excluido
                                                '' DDEUNRIS, --excluido
                                                '' TCONTAB, --PENDIENTE 06
                                                coalesce(prc.dpr_cap,0) VCAPITRC,
                                                '' VCAPRCMA, --excluido
                                                '' VCAPRCCO, --excluido
                                                '' VMTSELO, --descartado
                                                '' VMTFGAOU, --descartado
                                                '' VMTFGARC, --descartado
                                                '' KEBENTID_CH, --excluido
                                                '' DINDDESD, --excluido
                                                '' DARQUIVO, --excluido
                                                '' TARQUIVO, --excluido
                                                '' KRCINDREG, --excluido
                                                '' KRCINDORIG, --excluido
                                                '' KRCSUBES, --excluido
                                                '' KRCINDGBAN, --excluido
                                                '' DDESTPPROC, --excluido
                                                '' DCASHBACK --excluido 
                                from	(	select	prc.pre_id,
                                                                        prc.nbranch_led,
                                                                        prc.ncertif,
                                                                        case	when 	pre."NBRANCH" = 21 or not exists
                                                                                                        (	select	1
                                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                join	usvtimg01."GEN_COVER" gco
                                                                                                                                on		gco."NBRANCH" = pre."NBRANCH"
                                                                                                                                and		gco."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                and		gco."NMODULEC" = dp0."NMODULEC"
                                                                                                                                and		gco."NCOVER" = dp0."NDET_CODE"
                                                                                                                join	usvtimg01."TAB_GENCOV" tgc
                                                                                                                                on		tgc."NCOVERGEN" = gco."NCOVERGEN"
                                                                                                                                and		LOWER(tgc."SDESCRIPT") like 'r%civ%'
                                                                                                                where	dp0."NRECEIPT" = pre."NRECEIPT" 
                                                                                                                and		dp0."NDIGIT" = pre."NDIGIT"
                                                                                                                and		dp0."NBRANCH_LED" = prc.nbranch_led)
                                                                                        then	0
                                                                                        else	coalesce((	select	sum(coalesce(dp0."NCAPITAL" * case when dp0."SADDSUINI" in ('1','3') then 1 else 0 end,0))
                                                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                and		dp0."NBRANCH_LED" = nbranch_led),0) * ratio_prc_cer_con end dpr_cap,
                                                                        (	coalesce((	select 	coalesce("NSHARE",0)
                                                                                                        from	usvtimg01."COINSURAN" coi
                                                                                                        where	coi."SCERTYPE" = pre."SCERTYPE"
                                                                                                        and     coi."NBRANCH" = pre."NBRANCH"
                                                                                                        and     coi."NPRODUCT" = pre."NPRODUCT"
                                                                                                        and     coi."NPOLICY" = pre."NPOLICY"
                                                                                                        and 	coi."NCOMPANY" = 1
                                                                                                        and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                                                                        and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))),100)/100) ratio_coa,
                                                                        coalesce(prc.npremium_rea / nullif(prc.npremium_prc,0),0) ratio_rea,
                                                                        (npremium_dpr * ratio_prc_cer_cob) +
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."STYPE_DETAI" = '1'
                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        and		not exists
                                                                                                                        (	select 	1
                                                                                                                                from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                and		dp1."STYPE_DETAI" = '1'
                                                                                                                                and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_cob) dpr_cob,
                                                                        (npremium_dpr * ratio_prc_cer_rec) +
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."STYPE_DETAI" = '2'
                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        and		not exists
                                                                                                                        (	select 	1
                                                                                                                                from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                and		dp1."STYPE_DETAI" = '2'
                                                                                                                                and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_rec) dpr_rec,
                                                                        (npremium_dpr * ratio_prc_cer_des) +
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."STYPE_DETAI" in ('4','6')
                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        and		not exists
                                                                                                                        (	select 	1
                                                                                                                                from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                and		dp1."STYPE_DETAI" in ('4','6')
                                                                                                                                and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_des) dpr_des,
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."STYPE_DETAI" = '3'
                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        and		dp0."NBRANCH_LED" = nbranch_led),0) * ratio_prc_cer_con) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."STYPE_DETAI" = '3'
                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                        and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                        and		dp1."STYPE_DETAI" = '3'
                                                                                                                                        and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_con) dpr_imp,
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."NBILL_ITEM" = 4
                                                                                                        and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 4
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                        and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                        and		dp1."NBILL_ITEM" = 4)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_rgc,
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."NBILL_ITEM" = 5
                                                                                                        and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 5
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                        and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                        and		dp1."NBILL_ITEM" = 5)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_der,
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."NBILL_ITEM" = 9
                                                                                                        and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 9
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                        and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                        and		dp1."NBILL_ITEM" = 4)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_igv,
                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."NBILL_ITEM" = 97
                                                                                                        and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 97
                                                                                                                and		dp0."NBRANCH_LED" = nbranch_led
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimg01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                        and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                        and		dp1."NBILL_ITEM" = 97)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_gfi,
                                                                        (ncommision_dpr * ratio_prc_cer_com) +
                                                                        (	coalesce((	select	sum(coalesce(dp0."NCOMMISION",0))
                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                        and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        and		not exists
                                                                                                                        (	select 	1
                                                                                                                                from	usvtimg01."DETAIL_PRE" DP1
                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                and		dp1."NBRANCH_REI" = dp0."NBRANCH_REI" 
                                                                                                                                and		dp1."NMODULEC" = dp0."NMODULEC"
                                                                                                                                and		dp1."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_com) dpr_com
                                                        from	(	select	dpr.pre_id,
                                                                                                dpr.nbranch_led,
                                                                                                prc.ncertif,
                                                                                                sum(prc.npremium_prc * 
                                                                                                        (	case	
                                                                                                                        when	not exists
                                                                                                                                        (   SELECT  1
                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                WHERE   REI."SCERTYPE" = pre."SCERTYPE" and	REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                and		REI."NPRODUCT" =pre."NPRODUCT" and REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                and		REI."NCERTIF" in (0, prc.ncertif) and REI."NMODULEC" = prc.nmodulec
                                                                                                                                                and		REI."NBRANCH_REI" = prc.nbranch_rei and	CAST(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                        then 	100
                                                                                                                        else	(	select	min(coalesce("NSHARE",0))
                                                                                                                                                from	(	select	case	
                                                                                                                                                                                        when	NOT ("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                                                                        then	case	when	EXISTS
                                                                                                                                                                                                                                        (	SELECT  1
                                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                where	REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                                and		REI."NPRODUCT" =pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                                                and		REI."NCERTIF" = case when "SPOLITYPE" = '3' then 0 else prc.ncertif end
                                                                                                                                                                                                                                                and		REI."NMODULEC" = prc.nmodulec AND REI."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                                                                                                                                and		CAST(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                                                                        then	case	when	"SPOLITYPE" = '3'
                                                                                                                                                                                                                then	2
                                                                                                                                                                                                                else	1 end
                                                                                                                                                                                                                        else 	0 end 
                                                                                                                                                                                        when	("SPOLITYPE" = '2' AND pre."NBRANCH" = 57 AND pre."NPRODUCT" = 1)
                                                                                                                                                                                        then 	case	when	EXISTS
                                                                                                                                                                                                                                        (	select	1
                                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                where	REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                                and		REI."NPRODUCT" =pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                                                and		REI."NCERTIF" = coalesce(prc.ncertif,coalesce(pre."NCERTIF",0))
                                                                                                                                                                                                                                                and		REI."NMODULEC" = prc.nmodulec AND REI."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                                                                                                                                and		cast(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                                                                        then    1
                                                                                                                                                                                                                        else 	case 	when	EXISTS
                                                                                                                                                                                                                                                                        (   SELECT  1
                                                                                                                                                                                                                                                                                from	usvtimg01."REINSURAN" REI
                                                                                                                                                                                                                                                                                where	REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                                                                                                and		REI."NPRODUCT" = pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                                                                                                and		REI."NCERTIF" = 0 AND  REI."NMODULEC" = prc.nmodulec
                                                                                                                                                                                                                                                                                and		REI."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                                                                                                                                                                and		cast(REI."DEFFECDATE" as DATE) <= CAST(pre."DEFFECDATE" as DATE)
                                                                                                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR CAST(REI."DNULLDATE" as DATE) > CAST(pre."DEFFECDATE" as DATE)))
                                                                                                                                                                                                                THEN    2
                                                                                                                                                                                                                ELSE    0 END END
                                                                                                                                                                                ELSE    0 END FLAG_REA
                                                                                                                                                                                from 	(	select	pol."SPOLITYPE"
                                                                                                                                                                                                        from 	usvtimg01."POLICY" pol
                                                                                                                                                                                                        where	pol."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                        and		pol."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                        and		pol."NPRODUCT" = pre."NPRODUCT") pol) re0
                                                                                                                                                                                join	usvtimg01."REINSURAN" REI --* nivel revisado para el esquema de reaseguro asociado al registro
                                                                                                                                                                                                on		REI."SCERTYPE" = pre."SCERTYPE" AND REI."NBRANCH" = pre."NBRANCH"
                                                                                                                                                                                                and		REI."NPRODUCT" = pre."NPRODUCT" AND REI."NPOLICY" = pre."NPOLICY"
                                                                                                                                                                                                and		REI."NCERTIF" = case flag_rea when 1 then coalesce(prc.ncertif,coalesce(pre."NCERTIF",0)) when 2 then 0 end
                                                                                                                                                                                                and		REI."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                                                                                and		REI."NTYPE_REIN" = 1
                                                                                                                                                                                                and		cast(REI."DEFFECDATE" as date) <= cast(pre."DEFFECDATE" as date)
                                                                                                                                                                                                and		(REI."DNULLDATE" IS NULL OR cast(REI."DNULLDATE" as date) > cast(pre."DEFFECDATE" as date)))
                                                                                                                        end  /100)) npremium_rea,
                                                                                                sum(dpr.ncommision_dpr) ncommision_dpr,
                                                                                                sum(dpr.npremium_dpr) npremium_dpr,
                                                                                                sum(prc.npremium_prc) npremium_prc,
                                                                                                sum(dpr.dpr_porc_con) dpr_porc_con,
                                                                                                sum(dpr.dpr_porc_cob) dpr_porc_cob,
                                                                                                sum(dpr.dpr_porc_rec) dpr_porc_rec,
                                                                                                sum(dpr.dpr_porc_des) dpr_porc_des,
                                                                                                sum(dpr.dpr_porc_com) dpr_porc_com,
                                                                                                sum(coalesce(
                                                                                                        prc.npremium_prc /
                                                                                                        nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                and 	pr0."NCERTIF" is not null
                                                                                                                                and		pr0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                and 	pr0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                and		pr0."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                and		pr0."NMODULEC" = prc.nmodulec),0),0)) ratio_prc_cer_con,
                                                                                                sum(coalesce(
                                                                                                        prc.npremium_prc /
                                                                                                        nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                and 	pr0."NCERTIF" is not null
                                                                                                                                and		pr0."STYPE_DETAI" = '1'
                                                                                                                                and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                and 	pr0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                and		pr0."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                and		pr0."NMODULEC" = prc.nmodulec),0),0)) ratio_prc_cer_cob,
                                                                                                sum(coalesce(
                                                                                                        prc.npremium_prc /
                                                                                                        nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                and 	pr0."NCERTIF" is not null
                                                                                                                                and		pr0."STYPE_DETAI" = '2'
                                                                                                                                and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                and 	pr0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                and		pr0."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                and		pr0."NMODULEC" = prc.nmodulec),0),0)) ratio_prc_cer_rec,
                                                                                                sum(coalesce(
                                                                                                        prc.npremium_prc /
                                                                                                        nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                and 	pr0."NCERTIF" is not null
                                                                                                                                and		pr0."STYPE_DETAI" in ('4','6')
                                                                                                                                and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                and 	pr0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                and		pr0."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                and		pr0."NMODULEC" = prc.nmodulec),0),0)) ratio_prc_cer_des,
                                                                                                sum(coalesce(
                                                                                                        prc.ncommision_prc /
                                                                                                        nullif((	select	sum(coalesce(pr0."NCOMANUAL",0))
                                                                                                                                from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                and 	pr0."NCERTIF" is not null
                                                                                                                                and		pr0."STYPE_DETAI" in ('4','6')
                                                                                                                                and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                and 	pr0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                and		pr0."NBRANCH_REI" = prc.nbranch_rei
                                                                                                                                and		pr0."NMODULEC" = prc.nmodulec),0),0)) ratio_prc_cer_com
                                                                                from 	(	select	pre.ctid pre_id,
                                                                                                                        dpr."NBRANCH_REI" nbranch_rei,
                                                                                                                        dpr."NMODULEC" nmodulec,
                                                                                                                        dpr."NBRANCH_LED" nbranch_led,
                                                                                                                        sum(coalesce(dpr."NCOMMISION",0)) ncommision_dpr,
                                                                                                                        sum(coalesce(dpr."NPREMIUM",0)) npremium_dpr,
                                                                                                                        sum(coalesce(
                                                                                                                                coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                        and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_con,
                                                                                                                        sum(coalesce(
                                                                                                                                coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                        and		dp0."STYPE_DETAI" in ('1')
                                                                                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_cob,
                                                                                                                        sum(coalesce(
                                                                                                                                coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                        and		dp0."STYPE_DETAI" in ('2')
                                                                                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_rec,
                                                                                                                        sum(coalesce(
                                                                                                                                coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                        and		dp0."STYPE_DETAI" in ('4','6')
                                                                                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_des,
                                                                                                                        sum(coalesce(
                                                                                                                                coalesce(dpr."NCOMMISION",0) / 
                                                                                                                                nullif(	coalesce((	select	sum(coalesce(dp0."NCOMMISION",0))
                                                                                                                                                                        from	usvtimg01."DETAIL_PRE" dp0
                                                                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                        and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_com
                                                                                                        from	usvtimg01."PREMIUM" pre
                                                                                                        join 	usvtimg01."DETAIL_PRE" dpr
                                                                                                                        on		dpr."NRECEIPT" = pre."NRECEIPT"
                                                                                                                        and 	dpr."NDIGIT" = pre."NDIGIT"
                                                                                                                        and		dpr."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                        and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                                                        where	/*pre.ctid = '(48556,12)' and */exists
                                                                                                                        (	select 	1
                                                                                                                                from	usvtimg01."PREMIUM_CE" pr0
                                                                                                                                where 	pr0."NRECEIPT" = PRE."NRECEIPT")
                                                                                                        and 	PRE."NDIGIT" = 0 and pre."NBRANCH" = 57 --between 1 and 50
                                --									and pre."NRECEIPT" = 224782516
                                                                                                        AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2018'
                                                                                                        AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2018')
                                                                                                        AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2018')
                                                                                                        AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                                                                        group 	by 1,2,3,4) dpr
                                                                                join	(	select 	pre.ctid pre_id,
                                                                                                                        prc."NBRANCH_REI" nbranch_rei,
                                                                                                                        prc."NMODULEC" nmodulec,
                                                                                                                        prc."NCERTIF" ncertif,
                                                                                                                        sum(coalesce(prc."NPREMIUMN",0)) npremium_prc,
                                                                                                                        sum(coalesce(prc."NCOMANUAL",0)) ncommision_prc
                                                                                                        FROM 	usvtimg01."PREMIUM" pre
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
                                                                                                        WHERE 	PRE."NDIGIT" = 0 
                                                                                                        AND 	PRE."NBRANCH" = 57 
                                                                                                        AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2018'
                                                                                                        AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2018')
                                                                                                        AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2018')
                                                                                                        AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                                                                        group 	by 1,2,3,4) prc
                                                                                                on		prc.pre_id = dpr.pre_id
                                                                                                and		prc.nbranch_rei = dpr.nbranch_rei
                                                                                                and		prc.nmodulec = dpr.nmodulec
                                                                                join 	usvtimg01."PREMIUM" pre on pre.ctid = prc.pre_id
                                                                                group	by 1,2,3) prc
                                                        join 	usvtimg01."PREMIUM" pre on pre.ctid = prc.pre_id) prc
                                join 	usvtimg01."PREMIUM" pre on pre.ctid = prc.pre_id
                                join 	(select 'LPG' cia, '-' sep) par on 1 = 1 limit 200
                                --Tiempos desarrollo:
                                --7.842s (ramo 57)
                                --11m25s (otros ramos)
                                --Tiempos producci�n:
                                --2m33s (ramo 57)      
                              ) AS TMP
                              '''

    DF_LPG_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECPR_VTIME_LPG).load()

    L_RBRECPR_VTIME_LPV = f'''
                             (
                                        select	'D' as INDDETREC,
                                                'RBRECPR' as TABLAIFRS17,
                                                '' PK, 
                                                '' DTPREG, --excluido
                                                '' TIOCPROC, --excluido
                                                '' TIOCFRM, --excluido
                                                '' TIOCTO, --excluido
                                                '' KRITPREG, --PENDIENTE 01
                                                'PVV' KGIORIGM, --excluido
                                                pre."NBRANCH" KGCRAMO,
                                                pre."NPOLICY" DNUMAPO,
                                                coalesce(prc.ncertif,coalesce(pre."NCERTIF",0)) DNMCERT,
                                                pre."NRECEIPT" DNUMREC,
                                                '' DNMAGRE, 
                                                '' NSAGREG, --excluido
                                                '' KEBENTID, --excluido
                                                cast(pre."DISSUEDAT" as date) TEMISSAO, 
                                                cast(pre."DEFFECDATE" as date) TINICIO,
                                                cast(pre."DEXPIRDAT" as date) TTERMO,
                                                cast(pre."DSTATDATE" as date) TESTADO,
                                                coalesce (cast (cast(pre."DLIMITDATE" as date) as varchar ), '') TLIMCOB,
                                                case 	when	pre."NSTATUS_PRE" = '2'
                                                                then	(	select	coalesce (cast (cast (max("DSTATDATE") as date) as varchar),'')
                                                                                        from	usvtimv01."PREMIUM_MO"
                                                                                        where	"NRECEIPT" = pre."NRECEIPT"
                                                                                        and		"NDIGIT" = 0
                                                                                        and		"NTYPE" = 2)
                                                                else 	'' end TPGCOB, --SIN PERMISOS EN PROD. PREMIUM_MO (1)
                                                case 	when	pre."NSTATUS_PRE" = '3'
                                                                then	(	select	coalesce (cast (cast (max("DSTATDATE") as date) as varchar),'')
                                                                                        from	usvtimv01."PREMIUM_MO"
                                                                                        where	"NRECEIPT" = pre."NRECEIPT"
                                                                                        and		"NDIGIT" = 0
                                                                                        and		"NTYPE" = 7)
                                                                else 	'' end TANSUSP, --SIN PERMISOS EN PROD. PREMIUM_MO (2)
                                                coalesce (cast (cast(pre."DLIMITDATE" as date) + 1 as varchar),'')TDEVIDO,
                                                pre."NCURRENCY" KRCMOEDA,
                                                '' VCAMBIO, --descartado
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_gfi) VMTCOMR,
                                                (prc.dpr_rec + prc.dpr_des + prc.dpr_der) VMTENCG,
                                                (prc.dpr_imp + prc.dpr_igv) VMTIMPO,
                                                '' VMTPRMPR, --descartado
                                                '' VMTPRMBR, --descartado
                                                '' VMTPRMTR, --excluido
                                                '' VMTPRMAB, --descartado
                                                '' VMTJURO, --PENDIENTE 03
                                                '' VMTBONU, --excluido
                                                '' VMTDESC, --excluido
                                                '' VMTAGRA, --excluido
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_imp + prc.dpr_gfi) VMTTOTRP,
                                                '' VCAPITAL, --excluido
                                                '' KRCFMPGP, --descartado
                                                pre."NSTATUS_PRE" KRCESTRP,
                                                '' KRCMOSTP, --excluido
                                                pre."NTRATYPEI" KRCTPRCP,
                                                '' KCBMEDCB, --excluido
                                                '' KCBMEDCE, --excluido
                                                '' KCBMEDP2, --excluido
                                                '' KCBMEDRA, --excluido
                                                '' KRCESPRP, --excluido
                                                '' KRCTPCSG, --excluido
                                                '' KCBMEDPD, --excluido
                                                pre."NBRANCH" || par.sep || pre."NPRODUCT" KABPRODT,
                                                '' KRCTPFRC, --PENDIENTE 04
                                                prc.dpr_rgc VMTCOMCB,
                                                prc.dpr_com VMTCOMMD, 
                                                '' VMTCOMME, --excluido
                                                '' VMTCSAP, --excluido
                                                '' VMTCSCV, --escartado
                                                prc.dpr_gfi VMTCSFR,
                                                '' VMTIMPSL, --descartado
                                                '' VMTFAT, --descartado
                                                '' VMTFGA, --descartado
                                                '' VMTSNB, --descartado
                                                '' VMTINEM, --descartado
                                                '' VMTFUCA, --descartado
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_igv + prc.dpr_gfi) * prc.ratio_coa VMTCOSEG, 
                                                prc.dpr_gfi * prc.ratio_coa VMTCSTFC,
                                                (prc.dpr_cob + prc.dpr_rec + prc.dpr_des + prc.dpr_rgc + prc.dpr_der + prc.dpr_igv + prc.dpr_gfi) * prc.ratio_coa * 1 VMTRESSG, --no hay reaseguros en LPV VT
                                                '' VMTCMCCS, --descartado
                                                '' DUSRUPD, --excluido
                                                '' VMTCOMFD, --descartado
                                                '' VMTCOMPG, --descartado
                                                par.cia DCOMPA,
                                                '' DMARCA, --excluido
                                                '' KRBRECPR_MP, --excluido
                                                '' TMIGPARA, --excluido
                                                '' KRBRECPR_MD, --excluido
                                                '' TMIGDE, --excluido
                                                pre."NBRANCH" || par.sep || pre."NPOLICY" || par.sep || coalesce(prc.ncertif,coalesce(pre."NCERTIF",0)) KABAPOL,
                                                '' KABAPOL_EFT, --excluido
                                                '' KABAPOL_GRP, --excluido
                                                '' DNUMPRES, --PENDIENTE 05
                                                prc.nbranch_led KGCRAMO_SAP,
                                                '' KRCRGMDL, --excluido
                                                '' DNIB, --excluido
                                                '' DREFEATM, --excluido
                                                '' DINDRESG, --excluido
                                                '' TINICIOA, --excluido
                                                '' TTERMOA, --excluido
                                                '' DTERMO, --excluido
                                                '' DDEUNRIS, --excluido
                                                '' TCONTAB, --PENDIENTE 06
                                                0 VCAPITRC, --no existe el caso en LPV
                                                '' VCAPRCMA, --excluido
                                                '' VCAPRCCO, --excluido
                                                '' VMTSELO, --descartado
                                                '' VMTFGAOU, --descartado
                                                '' VMTFGARC, --descartado
                                                '' KEBENTID_CH, --excluido
                                                '' DINDDESD, --excluido
                                                '' DARQUIVO, --excluido
                                                '' TARQUIVO, --excluido
                                                '' KRCINDREG, --excluido
                                                '' KRCINDORIG, --excluido
                                                '' KRCSUBES, --excluido
                                                '' KRCINDGBAN, --excluido
                                                '' DDESTPPROC, --excluido
                                                '' DCASHBACK --excluido 
                                        from	(	select	prc.pre_id,
                                                                                prc.nbranch_led,
                                                                                prc.ncertif,
                                                                                (	coalesce((	select 	coalesce("NSHARE",0)
                                                                                                                from	usvtimv01."COINSURAN" coi
                                                                                                                where	coi."SCERTYPE" = pre."SCERTYPE"
                                                                                                                and     coi."NBRANCH" = pre."NBRANCH"
                                                                                                                and     coi."NPRODUCT" = pre."NPRODUCT"
                                                                                                                and     coi."NPOLICY" = pre."NPOLICY"
                                                                                                                and 	coi."NCOMPANY" = 1
                                                                                                                and     CAST(coi."DEFFECDATE" AS DATE) <= CAST(PRE."DEFFECDATE" AS DATE)
                                                                                                                and     (coi."DNULLDATE" is null or CAST(coi."DNULLDATE" AS DATE) > CAST(PRE."DEFFECDATE" AS DATE))),100)/100) ratio_coa,
                                                                                (npremium_dpr * ratio_prc_cer_cob) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."STYPE_DETAI" = '1'
                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."STYPE_DETAI" = '1'
                                                                                                                                        and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_cob) dpr_cob,
                                                                                (npremium_dpr * ratio_prc_cer_rec) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."STYPE_DETAI" = '2'
                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."STYPE_DETAI" = '2'
                                                                                                                                        and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_rec) dpr_rec,
                                                                                (npremium_dpr * ratio_prc_cer_des) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."STYPE_DETAI" in ('4','6')
                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."STYPE_DETAI" in ('4','6')
                                                                                                                                        and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_des) dpr_des,
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."STYPE_DETAI" = '3'
                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                and		dp0."NBRANCH_LED" = nbranch_led),0) * ratio_prc_cer_con) +
                                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                        from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		dp0."STYPE_DETAI" = '3'
                                                                                                                        and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                        and		not exists
                                                                                                                                        (	select 	1
                                                                                                                                                from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                                and		dp1."STYPE_DETAI" = '3'
                                                                                                                                                and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_con) dpr_imp,
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 4
                                                                                                                and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                        from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		dp0."NBILL_ITEM" = 4
                                                                                                                        and		not exists
                                                                                                                                        (	select 	1
                                                                                                                                                from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                                and		dp1."NBILL_ITEM" = 4)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_rgc,
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 5
                                                                                                                and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                        from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		dp0."NBILL_ITEM" = 5
                                                                                                                        and		not exists
                                                                                                                                        (	select 	1
                                                                                                                                                from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                                and		dp1."NBILL_ITEM" = 5)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_der,
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 9
                                                                                                                and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                        from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		dp0."NBILL_ITEM" = 9
                                                                                                                        and		not exists
                                                                                                                                        (	select 	1
                                                                                                                                                from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                                and		dp1."NBILL_ITEM" = 4)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_igv,
                                                                                (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."NBILL_ITEM" = 97
                                                                                                                and		dp0."NBRANCH_LED" = nbranch_led),0) * dpr_porc_con * ratio_prc_cer_con) +
                                                                                        (	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                        from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                        where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                        and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                        and		dp0."NBILL_ITEM" = 97
                                                                                                                        and		dp0."NBRANCH_LED" = nbranch_led
                                                                                                                        and		not exists
                                                                                                                                        (	select 	1
                                                                                                                                                from	usvtimv01."PREMIUM_CE" DP1
                                                                                                                                                where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                                and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                                and		dp1."NBILL_ITEM" = 97)),0) * dpr_porc_con * ratio_prc_cer_con) dpr_gfi,
                                                                                (ncommision_dpr * ratio_prc_cer_com) +
                                                                                (	coalesce((	select	sum(coalesce(dp0."NCOMMISION",0))
                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                and		not exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimv01."DETAIL_PRE" DP1
                                                                                                                                        where 	dp1."NRECEIPT" = dp0."NRECEIPT"
                                                                                                                                        and		dp1."NDIGIT" = dp0."NDIGIT" 
                                                                                                                                        and		dp1."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                        and		dp1."NBILL_ITEM" not in (4,5,9,97))),0) * ratio_prc_cer_com) dpr_com
                                                                from	(	select	dpr.pre_id,
                                                                                                        dpr.nbranch_led,
                                                                                                        prc.ncertif,
                                                                                                        sum(dpr.ncommision_dpr) ncommision_dpr,
                                                                                                        sum(dpr.npremium_dpr) npremium_dpr,
                                                                                                        sum(prc.npremium_prc) npremium_prc,
                                                                                                        sum(dpr.dpr_porc_con) dpr_porc_con,
                                                                                                        sum(dpr.dpr_porc_cob) dpr_porc_cob,
                                                                                                        sum(dpr.dpr_porc_rec) dpr_porc_rec,
                                                                                                        sum(dpr.dpr_porc_des) dpr_porc_des,
                                                                                                        sum(dpr.dpr_porc_com) dpr_porc_com,
                                                                                                        sum(coalesce(
                                                                                                                prc.npremium_prc /
                                                                                                                nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                        from	usvtimv01."PREMIUM_CE" pr0
                                                                                                                                        where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                        and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                        and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                        and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                        and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                        and 	pr0."NCERTIF" is not null
                                                                                                                                        and		pr0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                        and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                        and 	pr0."NDIGIT" = PRE."NDIGIT"),0),0)) ratio_prc_cer_con,
                                                                                                        sum(coalesce(
                                                                                                                prc.npremium_prc /
                                                                                                                nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                        from	usvtimv01."PREMIUM_CE" pr0
                                                                                                                                        where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                        and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                        and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                        and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                        and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                        and 	pr0."NCERTIF" is not null
                                                                                                                                        and		pr0."STYPE_DETAI" = '1'
                                                                                                                                        and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                        and 	pr0."NDIGIT" = PRE."NDIGIT"),0),0)) ratio_prc_cer_cob,
                                                                                                        sum(coalesce(
                                                                                                                prc.npremium_prc /
                                                                                                                nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                        from	usvtimv01."PREMIUM_CE" pr0
                                                                                                                                        where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                        and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                        and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                        and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                        and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                        and 	pr0."NCERTIF" is not null
                                                                                                                                        and		pr0."STYPE_DETAI" = '2'
                                                                                                                                        and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                        and 	pr0."NDIGIT" = PRE."NDIGIT"),0),0)) ratio_prc_cer_rec,
                                                                                                        sum(coalesce(
                                                                                                                prc.npremium_prc /
                                                                                                                nullif((	select	sum(coalesce(pr0."NPREMIUMN",0))
                                                                                                                                        from	usvtimv01."PREMIUM_CE" pr0
                                                                                                                                        where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                        and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                        and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                        and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                        and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                        and 	pr0."NCERTIF" is not null
                                                                                                                                        and		pr0."STYPE_DETAI" in ('4','6')
                                                                                                                                        and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                        and 	pr0."NDIGIT" = PRE."NDIGIT"),0),0)) ratio_prc_cer_des,
                                                                                                        sum(coalesce(
                                                                                                                prc.ncommision_prc /
                                                                                                                nullif((	select	sum(coalesce(pr0."NCOMANUAL",0))
                                                                                                                                        from	usvtimv01."PREMIUM_CE" pr0
                                                                                                                                        where	pr0."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                        and		pr0."NBRANCH" = pre."NBRANCH"
                                                                                                                                        and		pr0."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                        and		pr0."NPOLICY" = pre."NPOLICY"
                                                                                                                                        and		pr0."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                        and 	pr0."NCERTIF" is not null
                                                                                                                                        and		pr0."STYPE_DETAI" in ('4','6')
                                                                                                                                        and		pr0."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                        and 	pr0."NDIGIT" = PRE."NDIGIT"),0),0)) ratio_prc_cer_com
                                                                                        from 	(	select	pre.ctid pre_id,
                                                                                                                                dpr."NBRANCH_LED" nbranch_led,
                                                                                                                                sum(coalesce(dpr."NCOMMISION",0)) ncommision_dpr,
                                                                                                                                sum(coalesce(dpr."NPREMIUM",0)) npremium_dpr,
                                                                                                                                sum(coalesce(
                                                                                                                                        coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                        nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_con,
                                                                                                                                sum(coalesce(
                                                                                                                                        coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                        nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                                and		dp0."STYPE_DETAI" in ('1')
                                                                                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_cob,
                                                                                                                                sum(coalesce(
                                                                                                                                        coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                        nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                                and		dp0."STYPE_DETAI" in ('2')
                                                                                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_rec,
                                                                                                                                sum(coalesce(
                                                                                                                                        coalesce(dpr."NPREMIUM",0) / 
                                                                                                                                        nullif(	coalesce((	select	sum(coalesce(dp0."NPREMIUM",0))
                                                                                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                                and		dp0."STYPE_DETAI" in ('4','6')
                                                                                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_des,
                                                                                                                                sum(coalesce(
                                                                                                                                        coalesce(dpr."NCOMMISION",0) / 
                                                                                                                                        nullif(	coalesce((	select	sum(coalesce(dp0."NCOMMISION",0))
                                                                                                                                                                                from	usvtimv01."DETAIL_PRE" dp0
                                                                                                                                                                                where	dp0."NRECEIPT" = PRE."NRECEIPT"
                                                                                                                                                                                and		dp0."NDIGIT" = PRE."NDIGIT"
                                                                                                                                                                                and		dp0."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                                                                and		dp0."NBILL_ITEM" not in (4,5,9,97)),0),0),0)) dpr_porc_com
                                                                                                                from	usvtimv01."PREMIUM" pre
                                                                                                                join 	usvtimv01."DETAIL_PRE" dpr
                                                                                                                                on		dpr."NRECEIPT" = pre."NRECEIPT"
                                                                                                                                and 	dpr."NDIGIT" = pre."NDIGIT"
                                                                                                                                and		dpr."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                and		dpr."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                where	exists
                                                                                                                                (	select 	1
                                                                                                                                        from	usvtimv01."PREMIUM_CE" pr0
                                                                                                                                        where 	pr0."NRECEIPT" = PRE."NRECEIPT")
                                                                                                                and 	PRE."NDIGIT" = 0
                                                                                                                AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2018'
                                                                                                                AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2018')
                                                                                                                AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2018')
                                                                                                                AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                                                                                group 	by 1,2) dpr
                                                                                        join	(	select 	pre.ctid pre_id,
                                                                                                                                prc."NCERTIF" ncertif,
                                                                                                                                sum(coalesce(prc."NPREMIUMN",0)) npremium_prc,
                                                                                                                                sum(coalesce(prc."NCOMANUAL",0)) ncommision_prc
                                                                                                                FROM 	usvtimv01."PREMIUM" pre
                                                                                                                join 	usvtimv01."PREMIUM_CE" prc
                                                                                                                                on		prc."SCERTYPE" = pre."SCERTYPE"
                                                                                                                                and		prc."NBRANCH" = pre."NBRANCH"
                                                                                                                                and		prc."NPRODUCT" = pre."NPRODUCT"
                                                                                                                                and		prc."NPOLICY" = pre."NPOLICY"
                                                                                                                                and		prc."NRECEIPT" = PRE."NRECEIPT" 
                                                                                                                                and 	prc."NCERTIF" is not null
                                                                                                                                and		prc."STYPE_DETAI" in ('1','2','4','6')
                                                                                                                                and		prc."NBILL_ITEM" not in (4,5,9,97)
                                                                                                                                and 	prc."NDIGIT" = PRE."NDIGIT"
                                                                                                                WHERE 	PRE."NDIGIT" = 0
                                                                                                                AND 	CAST(PRE."DEFFECDATE" AS DATE) <= '12/31/2018'
                                                                                                                AND 	(PRE."DEXPIRDAT" IS NULL OR CAST(PRE."DEXPIRDAT" AS DATE) >= '12/31/2018')
                                                                                                                AND 	(PRE."DNULLDATE" IS NULL OR CAST(PRE."DNULLDATE" AS DATE) > '12/31/2018')
                                                                                                                AND 	PRE."SSTATUSVA" NOT IN ('2','3')
                                                                                                                group 	by 1,2) prc
                                                                                                        on		prc.pre_id = dpr.pre_id
                                                                                        join 	usvtimv01."PREMIUM" pre on pre.ctid = prc.pre_id
                                                                                        group	by 1,2,3) prc
                                                                join 	usvtimv01."PREMIUM" pre on pre.ctid = prc.pre_id) prc
                                        join 	usvtimv01."PREMIUM" pre on pre.ctid = prc.pre_id
                                        join 	(select 'LPV' cia, '-' sep) par on 1 = 1
                                        --462ms (desarrollo: todos los ramos)
                                        -- (producci�n: todos los ramos)
                             ) AS TMP
                             '''

    DF_LPV_VTIME = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECPR_VTIME_LPV).load()

    L_DF_RBRECPR_VTIME = DF_LPG_VTIME.union(DF_LPV_VTIME)

    L_RBRECPR_INSIS_LPV = f'''
                                      (SELECT 
                                       'D' AS INDDETREC
                                       ,'RBRECPR' AS TABLAIFRS17
                                       ,'' PK
                                       ,'' DTPREG
                                       ,'' TIOCPROC
                                       ,'' TIOCFRM
                                       ,'' TIOCTO
                                       ,'' KRITPREG
                                       ,'PNV' KGIORIGM
                                       ,B.RAMO_TECNICO KGCRAMO
                                       ,(SELECT CASE A.ENG_POL_TYPE
                                                      WHEN 'POLICY' THEN A.POLICY_ID
                                                       WHEN 'MASTER' THEN A.POLICY_ID
                                                ELSE  A.MASTER_POLICY_ID
                                                END) DNUMAPO
                                       ,(SELECT CASE A.ENG_POL_TYPE
                                                      WHEN 'DEPENDENT' THEN A.POLICY_ID
                                                ELSE  0
                                                END) DNMCERT
                                       ,A.NRO_DOCUMENTO DNUMREC
                                       ,A.REFERENCE_DOC DNMAGRE
                                       ,'' NSAGREG
                                       ,'' KEBENTID
                                       ,A.FECHA_EMISION_PROFORMA TEMISSAO
                                       ,A.FECHA_INI_PF TINICIO
                                       ,A.FECHA_FIN_PF TTERMO
                                       ,A.FECHA_ESTADO_PROFORMA  TESTADO
                                       ,A.FECHA_FIN_PF TLIMCOB
                                       ,CAST(A.FECHA_DOC_AUTORIZADO AS DATE) TPGCOB
                                       ,CAST(A.FECHA_EMISION_PROFORMA AS DATE) TANSUSP
                                       ,A.FECHA_VENCIMIENTO TDEVIDO
                                       ,A.MONEDA KRCMOEDA
                                       ,'' VCAMBIO
                                       ,'' VMTCOMR
                                       ,COALESCE((SELECT (GRC."DISCOUNT" * A.NETA/100) 
                                        FROM usinsiv01."GEN_RISK_COVERED" GRC 
                                        WHERE GRC."POLICY_ID" = B.POLICY_ID
                                        AND GRC."ANNEX_ID" = (SELECT MAX(GA."ANNEX_ID")
                                                            FROM usinsiv01."GEN_ANNEX" GA
                                                            WHERE GA."POLICY_ID" = GRC."POLICY_ID" 
                                                           )
                                       ),0) VMTENCG 
                                       ,A.IGV VMTIMPO
                                       ,A.NETA VMTPRMPR
                                       ,'' VMTPRMBR
                                       ,'' VMTPRMTR
                                       ,'' VMTPRMAB
                                       ,'' VMTJURO
                                       ,'' VMTBONU
                                       ,'' VMTDESC
                                       ,'' VMTAGRA
                                       ,A.PRIMA_TOTAL VMTTOTRP
                                       ,'' VCAPITAL
                                       ,'' KRCFMPGP
                                       ,A.ESTADO_PROFORMA KRCESTRP
                                       ,'' KRCMOSTP
                                       ,A.TIPO_PROFORMA KRCTPRCP
                                       ,'' KCBMEDCB
                                       ,'' KCBMEDCE
                                       ,'' KCBMEDP2
                                       ,'' KCBMEDRA
                                       ,'' KRCESPRP
                                       ,'' KRCTPCSG
                                       ,'' KCBMEDPD
                                       ,'' KABPRODT
                                       ,'' KRCTPFRC
                                       ,'' VMTCOMCB
                                       ,'' VMTCOMMD
                                       ,'' VMTCOMME
                                       ,'' VMTCSAP
                                       ,'' VMTCSCV
                                       ,'' VMTCSFR
                                       ,'' VMTIMPSL
                                       ,'' VMTFAT
                                       ,'' VMTFGA
                                       ,'' VMTSNB
                                       ,'' VMTINEM
                                       ,'' VMTFUCA
                                       ,'' VMTCOSEG
                                       ,'' VMTCSTFC
                                       ,'' VMTRESSG
                                       ,'' VMTCMCCS
                                       ,'' DUSRUPD
                                       ,'' VMTCOMFD
                                       ,'' VMTCOMPG
                                       ,'LPV' DCOMPA
                                       ,'' DMARCA
                                       ,'' KRBRECPR_MP
                                       ,'' TMIGPARA
                                       ,'' KRBRECPR_MD
                                       ,'' TMIGDE
                                       ,'' KABAPOL
                                       ,'' KABAPOL_EFT
                                       ,'' KABAPOL_GRP
                                       ,'' DNUMPRES
                                       ,'' KGCRAMO_SAP
                                       ,'' KRCRGMDL
                                       ,'' DNIB
                                       ,'' DREFEATM
                                       ,'' DINDRESG
                                       ,'' TINICIOA
                                       ,'' TTERMOA
                                       ,'' DTERMO
                                       ,'' DDEUNRIS
                                       ,'' TCONTAB
                                       ,'' VCAPITRC
                                       ,'' VCAPRCMA
                                       ,'' VCAPRCCO
                                       ,'' VMTSELO
                                       ,'' VMTFGAOU
                                       ,'' VMTFGARC
                                       ,'' KEBENTID_CH
                                       ,'' DINDDESD
                                       ,'' DARQUIVO
                                       ,'' TARQUIVO
                                       ,'' KRCINDREG
                                       ,'' KRCINDORIG
                                       ,'' KRCSUBES
                                       ,'' KRCINDGBAN
                                       ,'' DDESTPPROC
                                       ,'' DCASHBACK
                                       FROM (SELECT
                                                    P."POLICY_ID" POLICY_ID
                                                    ,PEP."ENG_POL_TYPE" ENG_POL_TYPE
                                                    ,PEP."MASTER_POLICY_ID" MASTER_POLICY_ID
                                                    ,CASE WHEN BDO."DOC_TYPE_ID" = 19997 THEN 'DEVOLUCIÓN'
                                                       WHEN BDO."DOC_TYPE_ID" = 19713 THEN 'COBRO'
                                                     END TIPO_PROFORMA
                                                    ,BDO."DOC_NUMBER" NRO_DOCUMENTO
                                                    ,COALESCE(CAST(BDO."REF_DOC_ID" AS VARCHAR) , '')REFERENCE_DOC
                                                    ,CAST(BDO."ISSUE_DATE" AS DATE) FECHA_EMISION_PROFORMA
                                                    ,MIN(COALESCE(CAST(M."ATTRIB_4" AS DATE), CAST(TR."ATTRIB_6" AS DATE))) FECHA_INI_PF
                                                    ,MAX(COALESCE(CAST(M."ATTRIB_5" AS DATE), CAST(TR."ATTRIB_7" AS DATE))) FECHA_FIN_PF
                                                    ,GREATEST (BDO."DUE_DATE", CAST(BDO."ISSUE_DATE" as DATE))FECHA_VENCIMIENTO
                                                    ,TR."CURRENCY" MONEDA
                                                    ,CASE
                                                         WHEN TR."PAID_STATUS" = 'Z' THEN
                                                              SUM(CASE WHEN TR."TRANSACTION_TYPE" = 'PREMIUM' THEN TR."AMOUNT" ELSE 0 END)
                                                         ELSE
                                                              SUM(CASE WHEN TR."TRANSACTION_TYPE" = 'PREMIUM' THEN M."AMOUNT" ELSE 0 END)
                                                     END NETA
                                                    ,CASE    
                                                         WHEN TR."PAID_STATUS" = 'Z' THEN
                                                              SUM(CASE WHEN TR."TRANSACTION_TYPE" = 'VAT' THEN TR."AMOUNT" ELSE 0 END)
                                                         ELSE
                                                              SUM(CASE WHEN TR."TRANSACTION_TYPE" = 'VAT' THEN M."AMOUNT" ELSE 0 END)
                                                     END IGV
                                                    ,CASE
                                                         WHEN TR."PAID_STATUS" = 'Z' THEN  SUM(TR."AMOUNT")
                                                         ELSE
                                                              SUM(M."AMOUNT")    
                                                     END PRIMA_TOTAL
                                                    ,CASE
                                                         WHEN TR."PAID_STATUS" IN ('N','P') THEN
                                                              'PENDIENTE'
                                                         WHEN TR."PAID_STATUS" = 'Y' THEN
                                                              'COBRADO'
                                                         WHEN TR."PAID_STATUS" = 'Z' THEN
                                                              'ANULADO'
                                                     END ESTADO_PROFORMA
                                                     ,CASE
                                                         WHEN TR."PAID_STATUS" IN ('N','P') AND BDO."DOC_SUFFIX" IS NULL THEN --PROFORMA PENDIENTE
                                                              CAST(BDO."DUE_DATE" AS DATE)
                                                         WHEN TR."PAID_STATUS" IN ('N','P') AND BDO."DOC_SUFFIX" IS NOT NULL THEN --PROFORMA PENDIENTE
                                                              COALESCE((SELECT  MAX(CAST(B."ACTION_DATE" AS DATE))            --- CORRECCIÓN EN LA FECHA DE DOC AUTORIZADO
                                                                        FROM usinsiv01."BLC_ACTIONS" B  
                                                                         WHERE B."DOCUMENT_ID" = BDO."DOC_ID"  
                                                                          AND B."ATTRIB_0" = BDO."DOC_SUFFIX"
                                                                          AND B."ACTION_TYPE_ID" = 21511
                                                                       ),CAST(BDO."DUE_DATE" AS DATE)
                                                                      )
                                                         WHEN MIN(TR."PAID_STATUS") IN ('Z') THEN  --PROFORMA ANULADA
                                                              (SELECT max(CAST(B."ACTION_DATE" AS DATE))
                                                               FROM usinsiv01."BLC_ACTIONS" B --ON (B.DOCUMENT_ID = A.DOC_ID)
                                                                WHERE  B."DOCUMENT_ID" = BDO."DOC_ID" --A.DOC_NUMBER = DO.DOC_NUMBER
                                                                AND B."ACTION_TYPE_ID"= 1410
                                                              )
                                                         /*WHEN MIN(TR."PAID_STATUS") IN ('Y') THEN --PROFORMA CANCELADA/ANULADA
                                                              COALESCE((SELECT DISTINCT  MAX(BP."PAYMENT_DATE")  
                                                                        FROM usinsiv01."BLC_PAYMENTS" BP  
                                                                        INNER JOIN usinsiv01."BLC_APPLICATIONS" BLCA 
                                                                         ON (BP."PAYMENT_ID" = BLCA."SOURCE_PAYMENT")
                                                                        INNER JOIN usinsiv01."BLC_TRANSACTIONS" BT_D 
                                                                         ON (BLCA."TARGET_TRX" = BT_D."TRANSACTION_ID")
                                                                        WHERE BT_D."DOC_ID" = BDO."DOC_ID"
                                                                         AND BP."STATUS" = 'C'
                                                                  ),CAST(BDO."DUE_DATE" AS DATE)
                                                                 )*/
                                                     END FECHA_ESTADO_PROFORMA
                                                    ,COALESCE(( SELECT MAX(CAST(B."ACTION_DATE" AS DATE))
                                                           FROM usinsiv01."BLC_ACTIONS" B
                                                           WHERE B."DOCUMENT_ID" = BDO."DOC_ID" -- IN(538105,702965)
                                                            AND B."ATTRIB_0" = BDO."DOC_SUFFIX"
                                                            AND B."ACTION_TYPE_ID" = 21511          
                                                         ),BDO."DUE_DATE"
                                                       ) FECHA_DOC_AUTORIZADO
                                              FROM usinsiv01."POLICY" P  
                                              INNER JOIN  usinsiv01."POLICY_ENG_POLICIES" PEP 
                                               ON PEP."POLICY_ID" = P."POLICY_ID"    
                                              LEFT JOIN usinsiv01."POLICY" PM 
                                               ON PM."POLICY_ID" = PEP."MASTER_POLICY_ID"
                                              JOIN usinsiv01."BLC_ITEMS" I 
                                               ON I."COMPONENT" = CAST(PEP."POLICY_ID" AS VARCHAR)
                                              JOIN usinsiv01."BLC_TRANSACTIONS" TR 
                                               ON  TR."ITEM_ID" = I."ITEM_ID"
                                              JOIN usinsiv01."BLC_DOCUMENTS" BDO 
                                               ON  BDO."DOC_ID" = TR."DOC_ID"
                                              LEFT JOIN usinsiv01."BLC_INSTALLMENTS" M 
                                               ON  M."TRANSACTION_ID" = TR."TRANSACTION_ID"   
                                               AND M."ITEM_ID" =  I."ITEM_ID" -- AND PEP.POLICY_ID  = M.POLICY)
                                              /*LEFT JOIN (SELECT B."DOC_ID",B."DOC_NUMBER", "POLICY_NO", "STATUS"
                                                                ,"ACTION_TYPE", "POLICY_CLASS", "DOC_REVERSE_DATE", A."ID"
                                                         FROM usinsiv01."BLC_PROFORMA_GEN" A 
                                                         JOIN (SELECT "DOC_NUMBER","DOC_ID",MAX("ID") AS ID
                                                               FROM usinsiv01."BLC_PROFORMA_GEN"
                                                               GROUP BY 1,2,"ACTION_TYPE"
                                                              ) B
                                                          ON B.ID = A."ID" 
                                                          AND B."DOC_NUMBER" = A."DOC_NUMBER"   
                                                          AND B."DOC_ID" = A."DOC_ID" 
                                                        ) BP
                                               ON BP."DOC_ID" = BDO."DOC_ID"  
                                                   AND BP."DOC_NUMBER" = BDO."DOC_NUMBER"  
                                                   AND BP."ACTION_TYPE" = ( CASE WHEN  TR."PAID_STATUS" IN ('N','P','Y') THEN  'CRE'
                                                                               WHEN  TR."PAID_STATUS"  = 'Z' THEN   'ANN'   
                                                                            END
                                                                          ) */
                                               WHERE P."INSR_END" BETWEEN '2021-01-01'  and '2021-12-31'
                                                AND BDO."DOC_CLASS" = 'B'
                                                AND BDO."DOC_TYPE_ID" IN (19997,19713)
                                              GROUP BY                                              
                                                    P."POLICY_ID",
                                                    PEP."ENG_POL_TYPE",
                                                    PEP."MASTER_POLICY_ID",
                                                    BDO."DOC_TYPE_ID",
                                                    BDO."DOC_NUMBER",
                                                    BDO."REF_DOC_ID",
                                                    BDO."ISSUE_DATE",
                                                    BDO."DUE_DATE",
                                                    TR."CURRENCY",
                                                    TR."PAID_STATUS",
                                                    BDO."DOC_SUFFIX",
                                                    BDO."DOC_ID"
--                                                    BDO."DOC_PREFIX",
--                                                    BDO."REFERENCE",
--                                                    BP."ACTION_TYPE",
--                                                    P."POLICY_NO",
--                                                    P."POLICY_NAME",
--                                                    BP."DOC_REVERSE_DATE",
--                                                    BP."POLICY_NO",
--                                                    P."REGISTRATION_DATE"
--                                                   ,P."INSR_BEGIN"
--                                                   ,P."INSR_END"
--                                                   ,PM."REGISTRATION_DATE"
--                                                   ,PM."INSR_BEGIN"
--                                                   ,PM."INSR_END"
--                                                   ,PM."POLICY_ID"
--                                                   ,BDO."ATTRIB_7"
--                                                   ,BP."STATUS"
--                                                   ,P."INSR_TYPE"
--                                                   ,P."ATTR8"
--                                                   ,BP."POLICY_CLASS"
--                                                   ,I."COMPONENT"
--                                                   ,P."POLICY_STATE"
--                                                   ,P."POLICY_STATE_AUX"
--                                                   ,BP."ID"
--                                                   ,TR."PAID_STATUS" 
                                           ) A
                                           LEFT JOIN (SELECT P."POLICY_ID" AS POLICY_ID
                                                            ,TB."TECHNICAL_BRANCH" AS  RAMO_TECNICO
                                                            ,P."INSR_TYPE" /*|| ' - ' || PN.INSR_TYPE_NAME*/AS  INSR_TYPE
                                                       FROM usinsiv01."POLICY" P
                                                    /*INNER JOIN usinsiv01."POLICY_NAMES" PN 
                                                      ON P."POLICY_ID" = PN."POLICY_ID"*/
                                                      INNER JOIN (SELECT DISTINCT CNP."PRODUCT_CODE", 
                                                                       PC."POLICY_ID",CP."PARAM_CPR_ID",
                                                                       CPV."PARAM_VALUE_CPR_ID", 
                                                                       CPV."DESCRIPTION" ,PC."COND_DIMENSION"
                                                                FROM usinsiv01."CFG_NL_PRODUCT" CNP
                                                                INNER JOIN usinsiv01."CFG_NL_PRODUCT_CONDS" CNPC 
                                                                  ON CNPC."PRODUCT_LINK_ID" = CNP."PRODUCT_LINK_ID"
                                                                INNER JOIN usinsiv01."CPR_PARAMS" CP 
                                                                  ON CP."PARAM_CPR_ID" = CNPC."PARAM_CPR_ID"
                                                                INNER JOIN usinsiv01."POLICY_CONDITIONS" PC 
                                                                  ON PC."COND_TYPE" = CP."PARAM_NAME" 
                                                                  AND PC."COND_TYPE" LIKE 'AS_IS%'
                                                                INNER JOIN usinsiv01."CPRS_PARAM_VALUE" CPV 
                                                                  ON CPV."PARAM_ID" = CP."PARAM_CPR_ID"   
                                                                  AND CPV."PARAM_VALUE" = PC."COND_DIMENSION"                
                                                               ) PROD 
                                                      on PROD."POLICY_ID" = P."POLICY_ID"  
                                                      AND PROD."PRODUCT_CODE" = P."INSR_TYPE"
                                                    INNER JOIN usinsiv01."CFGLPV_POLICY_TECHBRANCH_SBS" TB 
                                                      ON TB."INSR_TYPE" = PROD."PRODUCT_CODE"    
                                                      AND TB."AS_IS_PRODUCT"= cast(PROD."COND_DIMENSION" as INTEGER)
                                                      AND TB."TECHNICAL_BRANCH" = cast(P."ATTR1" as INTEGER)
                                                    LEFT JOIN usinsiv01."POLICY_PARTICIPANTS" PP 
                                                      ON PP."POLICY_ID" = P."POLICY_ID" 
                                                      AND PP."PARTICPANT_ROLE" = 'PHOLDER'  
                                                      AND PP."ANNEX_ID" = 0
                                                    WHERE P."INSR_END" BETWEEN '2021-01-01'  and '2021-12-31'
                                                     ) B
                                                   ON B.POLICY_ID = A.POLICY_ID
                                                   limit 100
                                      )AS TMP 
                                      '''

    L_DF_RBRECPR_INSIS = glue_context.read.format('jdbc').options(**connection).option("fetchsize",10000).option("dbtable",L_RBRECPR_INSIS_LPV).load()

    L_DF_RBRECPR = L_DF_RBRECPR_INSUNIX.union(L_DF_RBRECPR_VTIME).union(L_DF_RBRECPR_INSIS)

    return L_DF_RBRECPR