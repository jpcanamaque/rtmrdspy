class RTMQueries:

    @staticmethod
    def __getHourlyRTYData():
        return """
            SELECT
                    MAX(a.INSERTTIMESTAMP) AS INSERTTIMESTAMP,
                    CAST(MAX(UPPER(a.STRLOTNUMBER)) AS CHAR) AS STRLOTNUMBER,
                    MAX(UPPER(a.STRHOSTNAME)) AS STRHOSTNAME,
                    MAX(UPPER(a.STRTESTSTEP)) AS STRTESTSTEP,
                    MAX(a.STRTEST_COD) AS STRTEST_COD,
                    MAX(a.STREXTERNALPART) AS STREXTERNALPART,
                    MAX(a.INUMSITES) AS INUMSITES,
                    MAX(a.STRU2ID) AS STRU2ID,
                    MAX(a.LTOTALTESTED) AS LTOTALTESTED,
                    MAX(a.LTOTALGOOD) AS LTOTALGOOD,
                    MAX(a.LTOTALBAD) AS LTOTALBAD,
                    MAX(b.ISITE) AS SITE,
                    MAX(b.IBIN) AS BINS,
                    COALESCE(c.TESTERID, d.TESTERID, e.TESTERID) TESTERID,
                    MAX(a.STRPROGRAM) AS STRPROGRAM,
                    MAX(a.STRPROGRAMREV) AS STRPROGRAMREV
            FROM
                RTY_RAW.RTYPACKET AS a
            INNER JOIN RTY_RAW.RTYPACKET_BINS AS b ON a.BINS_ID = b.BINS_ID
            LEFT JOIN RTY_RAW.LK_TESTERID_HOSTNAME c ON
                a.STRHOSTNAME = c.HOSTNAME
            LEFT JOIN RTY_RAW.LK_TESTERID_HOSTNAME d ON
                a.STRHOSTNAME = d.HOSTNAME2
            LEFT JOIN RTY_RAW.LK_TESTERID_HOSTNAME e ON
                a.STRHOSTNAME = e.HOSTNAME3
            WHERE
                a.STRHOSTNAME LIKE '%%s%' 
                AND (
                    a.STRTESTSTEP LIKE CONCAT('%', 'FT', '%')
                    OR a.STRTESTSTEP LIKE CONCAT('%', 'FH', '%')
                    OR a.STRTESTSTEP LIKE CONCAT('%', 'FC', '%')
                    OR a.STRTESTSTEP LIKE CONCAT('%', 'FR', '%')
                )
                AND CHAR_LENGTH(a.STRLOTNUMBER) > 6
                AND a.STRLOTNUMBER NOT IN ('ENGINEERING', 'MODIFIED_DRIVER')
                AND a.LTOTALBAD >= 0
                AND a.INSERTTIMESTAMP BETWEEN %s AND %s
            GROUP BY a.STRTIMESTAMP , a.STRHOSTNAME , b.ISITE
            ORDER BY a.INSERTTIMESTAMP ASC
        """

    @staticmethod
    def __getCamstarLotDetails():
        return """
            SELECT * FROM (
                SELECT DISTINCT c.containername lotnum ,
                    la.attr_138 dietype ,
                    device,
                    packagetype ,
                    rd.attr_15 handlerid ,
                    rda.vendormodel handlertype,
                    we.TRACKINTIMESTAMP,
                    p.DESCRIPTION,
                    p.attr_033
                    FROM CONTAINER c
                    INNER JOIN a_lotattributes la
                    ON c.containerid = la.containerid
                    INNER JOIN product p
                    ON c.PRODUCTID = p.PRODUCTID
                    JOIN a_wipequipment we
                    ON c.containerid = we.containerid
                    JOIN resourcedef rd
                    ON we.equipmentid = rd.resourceid
                    JOIN
                    (SELECT vendormodel,attr_15 FROM resourcedef ORDER BY attr_02 DESC
                    ) rda
                    ON rda.attr_15 = rd.resourcename
                WHERE c.containername = :lotnumber
                UNION
                SELECT DISTINCT c.containername lotnum ,
                        la.attr_138 dietype ,
                        device,
                        packagetype ,
                        rd.attr_15 handlerid ,
                        rda.vendormodel handlertype,
                        we.TRACKINTIMESTAMP,
                        p.DESCRIPTION,
                        p.attr_033
                        FROM CONTAINER c
                        INNER JOIN a_lotattributes la
                        ON c.containerid = la.containerid
                        INNER JOIN product p
                        ON c.PRODUCTID = p.PRODUCTID
                        JOIN a_wipequipmenthistory we
                        ON c.containerid = we.containerid
                        JOIN resourcedef rd
                        ON we.equipmentid = rd.resourceid
                        JOIN
                        (SELECT vendormodel,attr_15 FROM resourcedef ORDER BY attr_02 DESC
                        ) rda
                        ON rda.attr_15 = rd.resourcename
                    WHERE c.containername = :lotnumber
                ORDER BY TRACKINTIMESTAMP DESC
                ) a
                WHERE ROWNUM = 1
        """

    @staticmethod
    def __getCamstarLotDetails2():
        return """
            SELECT DISTINCT c.containername lotnum ,
                la.attr_138 dietype ,
                device,
                packagetype ,
                rd.attr_15 handlerid ,
                rda.vendormodel handlertype,
                p.DESCRIPTION,
                p.attr_033
                FROM CONTAINER c
                INNER JOIN a_lotattributes la
                ON c.containerid = la.containerid
                INNER JOIN product p
                ON c.PRODUCTID = p.PRODUCTID
                JOIN a_wipequipment we
                ON c.containerid = we.containerid
                JOIN resourcedef rd
                ON we.equipmentid = rd.resourceid
                JOIN
                (SELECT vendormodel,attr_15 FROM resourcedef ORDER BY attr_02 DESC
                ) rda
                ON rda.attr_15 = rd.resourcename
            WHERE c.containername IN {}
        """

    @staticmethod
    def __getCamstarHWDetails():
        return """
            SELECT
                pn.paramnamename AS paramname,
                DECODE(pn.attr_001,'1',replace(ep.paramvalue,', ','|'),ep.paramvalue) AS paramvalue,
                nvl(pn.attr_001,'0') AS ishardware,
                r.resourcename
            FROM
                resourcedef r
                INNER JOIN a_equipmentparams ep ON r.resourceid = ep.resourceid
                INNER JOIN a_paramname pn ON ep.paramnameid = pn.paramnameid
            WHERE
                r.resourcename IN {}
                AND pn.attr_001 = 1
                AND pn.paramnamename NOT IN ('PROGRAMNAME', 'PROGRAMREV')
            ORDER BY
                pn.paramnamename
        """

    @staticmethod
    def __getHourlyRawRtyData():
        return """
            SELECT 
                a.INSERTTIMESTAMP,
                a.STRPROGRAM,
                a.STRPROGRAMREV,
                a.STRTESTSTEP,
                a.STRLOTNUMBER,
                a.STRHOSTNAME,
                CASE
					WHEN a.STRHOSTNAME LIKE '%%LTXMX%%' THEN 'LTXMX'
                    WHEN a.STRHOSTNAME LIKE '%%UFLEX%%' THEN 'MICROFLEX'
                    WHEN a.STRHOSTNAME LIKE '%%ETS88%%' THEN 'EAGLE'
				ELSE NULL
				END TESTERTYPE,
                a.STRSECTORID,
                a.INUMSITES,
                a.STRTEST_COD,
                COALESCE(c.TESTERID, d.TESTERID, e.TESTERID) TESTERID,
                CAST(GROUP_CONCAT(CONCAT('{"site":"', b.ISITE,'","bin":"',b.IBIN,'"}') ORDER BY b.ISITE SEPARATOR ';') AS CHAR) BIN_DETAILS
            FROM
                RTY_RAW.RTYPACKET a
                    LEFT JOIN
                RTY_RAW.RTYPACKET_BINS b ON a.BINS_ID = b.BINS_ID
                    LEFT JOIN 
                RTY_RAW.LK_TESTERID_HOSTNAME c ON a.STRHOSTNAME = c.HOSTNAME AND a.STRHOSTNAME NOT LIKE '%%ETS%%'
		            LEFT JOIN
                RTY_RAW.LK_TESTERID_HOSTNAME d ON a.STRHOSTNAME = d.HOSTNAME2 AND a.STRHOSTNAME NOT LIKE '%%ETS%%'
		            LEFT JOIN
                RTY_RAW.LK_TESTERID_HOSTNAME e ON a.STRHOSTNAME = e.HOSTNAME3 AND a.STRHOSTNAME NOT LIKE '%%ETS%%'
            WHERE
                1
                AND (
                    a.STRHOSTNAME LIKE '%%UFLEX%%' 
                    OR a.STRHOSTNAME LIKE '%%LTXMX%%'
                    OR a.STRHOSTNAME LIKE '%%ETS88%%'
                )
                AND (
                    a.STRTESTSTEP LIKE '%%FT%%'
                    OR a.STRTESTSTEP LIKE '%%FH%%'
                    OR a.STRTESTSTEP LIKE '%%FC%%'
                    OR a.STRTESTSTEP LIKE '%%FR%%'
                )
                AND CHAR_LENGTH(a.STRLOTNUMBER) > 6
		        AND (
                    a.STRLOTNUMBER NOT IN ('ENGINEERING', 'MODIFIED_DRIVER', 'TESTING')
                    AND a.STRLOTNUMBER NOT LIKE '%%TEST%%'
                    AND a.STRLOTNUMBER NOT LIKE '%%TST%%'
                )
                -- AND a.STRLOTNUMBER IN ('NBJC0A574A', 'QCCI2A321A')
                AND a.INSERTTIMESTAMP BETWEEN %s AND %s
                AND b.BINS_ID IS NOT NULL
            GROUP BY a.BINS_ID
            ORDER BY a.STRLOTNUMBER, a.STRTIMESTAMP DESC
        """

    @staticmethod
    def __getCamstarLimitDetails():
        return """
            SELECT
                pb.PRODUCTNAME DEVICE,
                yy.yieldtypename,
                sb.specname,
                c.containername lotnum,
                MAX(ROUND(yl.LowerYieldLimit, 2)) SYL,
                MAX(
                    CASE
                    WHEN bld.Bin = '5' THEN bl.LimitInPercentage
                    ELSE 1
                    END
                ) AS SBL5,
                MAX(
                    CASE
                    WHEN bld.Bin = '6' THEN bl.LimitInPercentage
                    ELSE 1
                    END
                ) AS SBL6,
                MAX(
                    CASE
                    WHEN bld.Bin = '12' THEN bl.LimitInPercentage
                    ELSE 0
                    END
                ) AS SBL12
            FROM A_YieldLimits yl
                INNER JOIN product p ON p.productid = yl.productid
                INNER JOIN productbase pb ON p.PRODUCTBASEID = pb.PRODUCTBASEID
                LEFT OUTER JOIN A_BinsLimits bl ON yl.YieldLimitsId = bl.YieldLimitsId
                LEFT OUTER JOIN A_BinsLimitsDetails bld ON bl.BinsLimitsId = bld.BinsLimitsId
                INNER JOIN spec s ON yl.SpecId = s.SpecId
                INNER JOIN specbase sb ON sb.revofrcdid = s.specid
                LEFT OUTER JOIN A_yieldtype yy ON yl.yieldtypeid = yy.yieldtypeid
                LEFT JOIN container c ON c.CONTAINERID = yl.LOTID
            WHERE
                pb.PRODUCTNAME IN {}
                AND yy.yieldtypename = 'FTELECTRICAL'
            GROUP BY
                pb.PRODUCTNAME,
                yy.yieldtypename,
                sb.specname,
                c.containername
        """

    @staticmethod
    def __getEquipmentHistory():
        return """
            SELECT
                DISTINCT
                    TRANSACTIONDATE,
                    RESOURCESTATUSCODE,
                    RESOURCESTATUSREASON,
                    EQUIPMENT,
                    LOTNUM
            FROM
                CAM_EQUIPMENTEVENT_HIST A
            WHERE
                A.EQUIPMENT = %s
                AND A.LOTNUM = %s
                AND (A.RESOURCESTATUSCODE <> '' AND A.RESOURCESTATUSREASON <> '')
                AND A.TRANSACTIONDATE <= %s
            ORDER BY TRANSACTIONDATE DESC
        """

    @staticmethod
    def __setRTMSetupDetails():
        return """
            INSERT INTO `rtm_tester_details`
                (
                    `active_tester_id`,
                    `area`,
                    `lotnum`,
                    `hostname`,
                    `testerid`,
                    `handlerid`,
                    `testertype`,
                    `handlertype`,
                    `teststep`,
                    `rootdie`,
                    `dietype`,
                    `rootdevice`,
                    `device`,
                    `numsites`,
                    `packagetype`,
                    `testprogram`,
                    `programrev`,
                    `test_code`,
                    `description`,
                    `attr_033`
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                ) 
                ON DUPLICATE KEY UPDATE `last_updated` = NOW()
        """
    @staticmethod
    def __setRTMSetupHistory():
        return """
            INSERT IGNORE INTO `rtm_testers_hist`
            (
                `rtm_testers_site_id`,
                `active_tester_id`,
                `date_snipped`,
                `shift`,
                `total_good`,
                `total_tested`,
                `upperbin_pct`,
                `lowerbin_pct`,
                `syl`,
                `sbl5`,
                `sbl6`,
                `sbl12`,
                `isbelowsamplesize`,
                `bin5_cnt`,
                `bin6_cnt`,
                `bin12_cnt`
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """

    @staticmethod
    def __setRTMSetupSiteDetails():
        return """
            INSERT IGNORE INTO `rtm_testers_site_details`
            (
                `rtm_testers_site_id`,
                `sitenum`,
                `bin_num`,
                `bin_cnt`
            ) VALUES (
                %s,%s,%s,%s
            )
        """
    @staticmethod
    def __setRTMFailLogDetails():
        return """
            INSERT IGNORE INTO `rtm_fail_logs`
            (
                `flid`,
                `active_tester_hist_id`,
                `fail_type`,
                `system_calc_mtf`,
                `system_mtf_site`,
                `ismainmtf`
            ) VALUES (
                %s,%s,%s,%s,%s,%s
            )
        """

    @staticmethod
    def __setRTMSetupHardwareDetails():
        return """
            INSERT IGNORE INTO `rtm_hardware_data`
            (
                `rtm_testers_site_id`,
                `hardware_type`,
                `hardware_value`
            ) VALUES (
                %s,%s,%s
            )
        """