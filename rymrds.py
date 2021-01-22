### RTM Summarizer custom modules
from database import Database
from queries import RTMQueries
from config import RTMConfig as config
from RTMExceptions import *

### Python native modules
from collections import Counter 
from datetime import datetime
from pprint import pprint
from hashlib import md5
import uuid
import json
import logging

### Initializing RTM Custom Exceptions
RtmPacketNotProdn = RTMPacketNotProdn()

class RTMRawDataSummarizer(object):
    """
        Main classs for the RTM Raw Data summarizer
    """
    def __init__ (self, params):
        self.params = params
        self.db_connections =  {
            'rty_db' : Database.dbConnect("MPOC_RTY_DB"),
            'camstar_db' : Database.dbConnect("MPOC_CAMSTAR_DB"),
            'bridge_db' : Database.dbConnect("MPOC_BRIDGE_DB"),
            'rtm_db' : Database.dbConnect("MPOC_RTM_DB"),
        }

    def __del__ (self):
        self.db_connections = {}

    def execute(self):
        rtm_data = self.summarize()
        self.saveSetups(rtm_data)
        return rtm_data

    def summarize(self):
        processed_lots = []
        testerids = []
        rtm_info = {}
        eqpt_hist_info = {}
        
        prev_hr = datetime.strptime(self.params['startdate'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:00:00")
        curr_hr = datetime.strptime(self.params['enddate'], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:00:00")
        
        logging.info("Getting RTY data between %s and %s", self.params.get('startdate'), self.params.get('enddate'))
        stmt = self.db_connections['rty_db']['cred'].cursor()
        stmt.execute(RTMQueries._RTMQueries__getHourlyRawRtyData(), (self.params.get('startdate'), self.params.get('enddate')))

        for data in stmt:
            lotnum = data.get("STRLOTNUMBER").split("_")[0]
            if len(lotnum) < config.MIN_LOT_CHAR_LENGTH: continue

            instimestamp_hr = data.get("INSERTTIMESTAMP").strftime("%Y-%m-%d %H:00:00")
            
            testerid = str(data.get("TESTERID")).upper()
            if data.get("TESTERTYPE") == "EAGLE":
                testerid = "EAGLE88" + data.get("STRHOSTNAME").split("-")[1][0:3] + data.get("STRSECTORID")[1:]
                sample_size = config.EAGLE_SAMPLE_SIZE
            elif data.get("TESTERTYPE") == "MICROFLEX":
                sample_size = config.MICROFLEX_SAMPLE_SIZE
            elif data.get("TESTERTYPE") == "LTXMX":
                sample_size = config.LTXMX_SAMPLE_SIZE

            if testerid not in testerids:
                testerids.append(testerid)

            if lotnum not in processed_lots:
                setupid = uuid.uuid3(uuid.NAMESPACE_DNS, lotnum + str(data.get('TESTERID')) + str(data.get('STRTESTSTEP'))).hex
                is_prodn_data = True
                rtm_info[lotnum] = {
                    'setupid' : setupid
                    , 'isbelowsamplesize' : 0
                    , 'isfullprod' : True
                    , 'area' : 'FT'
                    , 'testprogram' : data.get('STRPROGRAM')
                    , 'programrev' : data.get('STRPROGRAMREV')
                    , 'teststep' : data.get('STRTESTSTEP')
                    , 'test_code' : data.get('STRTEST_COD')
                    , 'hostname' : data.get('STRHOSTNAME').upper()
                    , 'testerid' : testerid
                    , 'testertype' : data.get('TESTERTYPE')
                    , 'numsites' : data.get('INUMSITES')
                    , 'lotnum' : lotnum
                    , 'sample_size' : sample_size
                    , 'bin_details' : {
                        prev_hr : {
                            'good_bins' : 0
                            , 'total_bins' : 0
                            , 'bin5_count' : 0
                            , 'bin6_count' : 0
                            , 'bin12_count' : 0
                            , 'site_count' : {}
                        }, 
                        curr_hr : {
                            'good_bins' : 0
                            , 'total_bins' : 0
                            , 'bin5_count' : 0
                            , 'bin6_count' : 0
                            , 'bin12_count' : 0
                            , 'site_count' : {}
                        }
                    }
                }
                processed_lots.append(lotnum)
                eqpt_hist_info[lotnum] = self.__getEquipmentHistory(data.get('TESTERID'), lotnum, self.params.get('snipdate'))
            
            try:
                if is_prodn_data :
                    if len(eqpt_hist_info[lotnum]) < 1 : 
                        is_prodn_data = False
                        raise RTMPacketNotProdn
                    
                    if eqpt_hist_info[lotnum][0]['RESOURCESTATUSCODE'] != 'PRODN' or \
                        eqpt_hist_info[lotnum][0]['RESOURCESTATUSREASON'] not in config.PRODUCTION_STATUS_REASON:                       
                        is_prodn_data = False
                        raise RtmPacketNotProdn

                    for eqpt_hist in eqpt_hist_info[lotnum]:
                        if data.get("INSERTTIMESTAMP") >= eqpt_hist.get("TRANSACTIONDATE"):
                            if (
                                    eqpt_hist['RESOURCESTATUSCODE'] != 'PRODN' or \
                                    eqpt_hist['RESOURCESTATUSREASON'] != 'PRODUCTION'
                                ) and \
                                (
                                    eqpt_hist['RESOURCESTATUSCODE'] != 'PRODN' or \
                                    eqpt_hist['RESOURCESTATUSREASON'] != 'DISABLED SITE'
                                ) and \
                                (
                                    eqpt_hist['RESOURCESTATUSCODE'] != 'PRODN' or \
                                    eqpt_hist['RESOURCESTATUSREASON'] != 'DISABLED SITE - HANDLER'
                                ):
                                is_prodn_data = False
                                rtm_info[lotnum]['isfullprod'] = False
                                raise RtmPacketNotProdn
                            break
                else:
                    raise RTMPacketNotProdn
            except RTMPacketNotProdn:
                continue
            
            bin_details = list(map(json.loads, data.get('BIN_DETAILS').split(";")))
            for bin_detail in bin_details:
                if bin_detail["site"] not in rtm_info[lotnum]['bin_details'][instimestamp_hr]["site_count"]:
                    rtm_info[lotnum]['bin_details'][instimestamp_hr]["site_count"][bin_detail["site"]] = {"good" : 0, "total" : 0}
                
                rtm_info[lotnum]['bin_details'][instimestamp_hr]['total_bins']+=1
                rtm_info[lotnum]['bin_details'][instimestamp_hr]["site_count"][bin_detail["site"]]["total"]+=1
                if bin_detail["bin"] in config.GOOD_BIN_NUMBERS:
                    rtm_info[lotnum]['bin_details'][instimestamp_hr]['good_bins']+=1
                    rtm_info[lotnum]['bin_details'][instimestamp_hr]["site_count"][bin_detail["site"]]["good"]+=1
                
                if bin_detail["bin"] == "5":
                    rtm_info[lotnum]['bin_details'][instimestamp_hr]['bin5_count']+=1
                if bin_detail["bin"] == "6":
                    rtm_info[lotnum]['bin_details'][instimestamp_hr]['bin6_count']+=1
                if bin_detail["bin"] == "12":
                    rtm_info[lotnum]['bin_details'][instimestamp_hr]['bin12_count']+=1

                if bin_detail["bin"] not in rtm_info[lotnum]['bin_details'][instimestamp_hr]["site_count"][bin_detail["site"]]:
                    rtm_info[lotnum]['bin_details'][instimestamp_hr]["site_count"][bin_detail["site"]][bin_detail["bin"]] = 0
                rtm_info[lotnum]['bin_details'][instimestamp_hr]["site_count"][bin_detail["site"]][bin_detail["bin"]]+=1

        logging.info("Getting Camstar Lot details for %s lots", len(rtm_info))
        camstar_info = self.__getCamstarLotDetails(processed_lots)       

        for info in camstar_info:
            if info['LOTNUM'] not in rtm_info:
                continue
            rtm_info.get(info['LOTNUM'])['dietype'] = info.get('DIETYPE')
            rtm_info.get(info['LOTNUM'])['rootdie'] = info.get('DIETYPE')[0:4]
            rtm_info.get(info['LOTNUM'])['device'] = info.get('DEVICE')
            rtm_info.get(info['LOTNUM'])['packagetype'] = info.get('PACKAGETYPE')
            rtm_info.get(info['LOTNUM'])['handlerid'] = info.get('HANDLERID')
            rtm_info.get(info['LOTNUM'])['handlertype'] = info.get('HANDLERTYPE')
            rtm_info.get(info['LOTNUM'])['description'] = info.get('DESCRIPTION')
            rtm_info.get(info['LOTNUM'])['attr_033'] = info.get('ATTR_033')
        
        devices = list(set(map(lambda x : x.get("DEVICE"), camstar_info)))
        logging.info("Getting Camstar Limit details for %s device/s", len(devices))
        syl_info = self.__getCamstarLimits(devices)

        logging.info("Getting Camstar Hardware details for %s testers", len(testerids))
        hardware_info = self.__getCamstarHardwareDetails(testerids)

        for a in list(rtm_info.values()):
            if a.get('handlerid') == None:
                logging.info("Removing %s on the summary data, no Camstar Lot detail available", a.get('lotnum'))
                del rtm_info[a.get('lotnum')]
                continue

            if a.get('testerid') in list(hardware_info.keys()):
                rtm_info[a.get('lotnum')]['hardware_info'] = dict(hardware_info.get(a.get('testerid')))

            new_bin_details = {
                'good_bins' : 0
                , 'total_bins' : 0
                , 'bin5_count' : 0
                , 'bin6_count' : 0
                , 'bin12_count' : 0
                , 'site_count' : {}
            }
            curr_bin_details = a['bin_details'][curr_hr]
            prev_bin_details = a['bin_details'][prev_hr]
            prev_tested_per_site = list(map(lambda x: x.get("total", 0), list(prev_bin_details['site_count'].values()))) or [0]
            curr_tested_per_site = list(map(lambda x: x.get("total", 0), list(curr_bin_details['site_count'].values()))) or [0]
            if min(curr_tested_per_site) > 0 and min(curr_tested_per_site) < a["sample_size"]:
                new_bin_details["good_bins"] = curr_bin_details.get("good_bins", 0) + prev_bin_details.get("good_bins", 0)
                new_bin_details["total_bins"] = curr_bin_details.get("total_bins", 0) + prev_bin_details.get("total_bins", 0)
                new_bin_details["bin5_count"] = curr_bin_details.get("bin5_count", 0) + prev_bin_details.get("bin5_count", 0)
                new_bin_details["bin6_count"] = curr_bin_details.get("bin6_count", 0) + prev_bin_details.get("bin6_count", 0)
                new_bin_details["bin12_count"] = curr_bin_details.get("bin12_count", 0) + prev_bin_details.get("bin12_count", 0)
                new_bin_details['site_count'] = dict([(k, dict(Counter(curr_bin_details['site_count'].get(k, {"good":0, "total":0})) 
                    + Counter(prev_bin_details['site_count'].get(k, {"good":0, "total":0})))) 
                    for (k) in set(curr_bin_details['site_count'].keys()) | set(prev_bin_details['site_count'].keys())])

                rtm_info[a.get('lotnum')]['bin_details'] = new_bin_details
            else:
                rtm_info[a.get('lotnum')]['bin_details'] = curr_bin_details

            if min(list(map(lambda x: x.get("total", 0), list(rtm_info[a.get('lotnum')]['bin_details'].get('site_count').values() or [{}] )))) < a.get("sample_size"):
                rtm_info[a.get('lotnum')]['isbelowsamplesize'] = 1

            rtm_info[a.get('lotnum')]['syl'] = config.DEFAULT_SYL
            rtm_info[a.get('lotnum')]['sbl5'] = config.DEFAULT_SBL5
            rtm_info[a.get('lotnum')]['sbl6'] = config.DEFAULT_SBL6
            rtm_info[a.get('lotnum')]['sbl12'] = config.DEFAULT_SBL12

            if a.get("device") is not None and a.get("test_code") not in config.EXCLUDE_OPERCODE:
                opercode = a.get("test_code").split(",")[0].split('=')
                opercode = opercode[1] if len(opercode) > 1 else opercode[0]
                limits = [l for l in syl_info if l['DEVICE'] == a.get("device") and l['SPECNAME'].startswith(opercode)]
                if len(limits) > 0:
                    rtm_info[a.get('lotnum')]['syl'] = limits[0].get('SYL')
                    rtm_info[a.get('lotnum')]['sbl5'] = limits[0].get('SBL5')
                    rtm_info[a.get('lotnum')]['sbl6'] = limits[0].get('SBL6')
                    rtm_info[a.get('lotnum')]['sbl12'] = limits[0].get('SBL12')

        return rtm_info

    def saveSetups(self, rtm_data):
        with self.db_connections['rtm_db']["cred"].cursor() as cursor:
            for data in rtm_data.values():
                params = (
                    data.get('setupid'),
                    data.get('area'),
                    data.get('lotnum'),
                    data.get('hostname'),
                    data.get('testerid'),
                    data.get('handlerid'),
                    data.get('testertype'),
                    data.get('handlertype'),
                    data.get('teststep'),
                    data.get('rootdie'),
                    data.get('dietype'),
                    "",
                    data.get('device'),
                    data.get('numsites'),
                    data.get('packagetype'),
                    data.get('testprogram'),
                    data.get('programrev'),
                    data.get('test_code'),
                    data.get('description'),
                    data.get('attr_033'),
                )
                logging.info("Saving RTM setup details for %s ...", data.get('lotnum'))
                cursor.execute(RTMQueries._RTMQueries__setRTMSetupDetails(), params)
                self.db_connections['rtm_db']["cred"].commit()

                self.saveRTMHistory(data)
                logging.info("Saving RTM setup details for %s saved", data.get('lotnum'))
        return
    
    def saveRTMHistory(self, setup_details):
        with self.db_connections['rtm_db']["cred"].cursor() as cursor:
            logging.info("Saving RTM setup history for %s ..", setup_details.get('lotnum'))
            histid = uuid.uuid3(uuid.NAMESPACE_DNS, setup_details.get('setupid') + str(self.params.get('snipdate'))).hex
            total_good = setup_details.get('bin_details').get('good_bins')
            total_tested = setup_details.get('bin_details').get('total_bins')
            bin5_count = setup_details.get('bin_details').get('bin5_count')
            bin6_count = setup_details.get('bin_details').get('bin6_count')
            bin12_count = setup_details.get('bin_details').get('bin12_count')
            site_yield = list(map(lambda s : s.get('good', 0) / s.get('total', 0) * 100, setup_details.get('bin_details').get('site_count').values()))
            upperbinpct = 0 if len(site_yield) < 1 else max(site_yield)
            lowerbinpct = 0 if len(site_yield) < 1 else min(site_yield)
            syl = setup_details.get('syl')
            sbl5 = setup_details.get('sbl5')
            sbl6 = setup_details.get('sbl6')
            sbl12 = setup_details.get('sbl12')
            isbelowsamplesize = setup_details.get('isbelowsamplesize')
            
            ### Shifting based on Maxim Calendar = Morning(6am to 5:59pm) Night(6pm to 5:59am the following day)
            date_snip_hr = datetime.strptime(self.params.get('snipdate'), "%Y-%m-%d %H:%M:%S").hour
            if date_snip_hr >= 6 and date_snip_hr <= 17:
                shift = 1 ### Morning Shift
            elif date_snip_hr < 6 or date_snip_hr >= 18:
                shift = 0 ### Night Shift
            params = (
                histid,
                setup_details.get('setupid'),
                self.params.get('snipdate'),
                shift,
                total_good,
                total_tested,
                upperbinpct,
                lowerbinpct,
                syl,
                sbl5,
                sbl6,
                sbl12,
                isbelowsamplesize,
                bin5_count,
                bin6_count,
                bin12_count
            )
            cursor.execute(RTMQueries._RTMQueries__setRTMSetupHistory(), params)
            self.db_connections['rtm_db']["cred"].commit()
            logging.info("Saving RTM setup history for %s saved", setup_details.get('lotnum'))
            
            logging.info("Saving RTM setup site details for %s ..", setup_details.get('lotnum'))
            self.saveRTMSiteDetails(histid, setup_details.get('bin_details'))

            logging.info("Checking RTM setup %s for site variance violation", setup_details.get('lotnum'))
            if upperbinpct - lowerbinpct >= config.DEFAULT_SITEVAR:
                logging.info("Site Variance detected for %s, computing MTF ... ", setup_details.get('lotnum'))
                self.saveRTMSiteVarianceFailLogData(histid, setup_details)

            try:
                logging.info("Checking RTM setup %s for SYL violation", setup_details.get('lotnum'))
                if (total_good / total_tested * 100) <= syl:
                    logging.info("SYL violation detected for %s, Spot Yield = %s | SYL = %s", setup_details.get('lotnum'), total_good / total_tested * 100, syl)
                    self.saveRTMSylFailLogData(histid, setup_details)
                
                logging.info("Checking RTM setup %s for Bin 5 SBL violation", setup_details.get('lotnum'))
                if (bin5_count / total_tested * 100) >= sbl5:
                    logging.info("Bin 5 SBL violation detected for %s, Bin 5 = %s | SBL5 = %s", setup_details.get('lotnum'), bin5_count / total_tested * 100, sbl5)
                    self.saveRTMSblFailLogData(histid, setup_details, 'SBL5')

                logging.info("Checking RTM setup %s for Bin 6 SBL violation", setup_details.get('lotnum'))
                if (bin6_count / total_tested * 100) >= sbl6:
                    logging.info("Bin 6 SBL violation detected for %s, Bin 6 = %s | SBL6 = %s", setup_details.get('lotnum'), bin6_count / total_tested * 100, sbl6)
                    self.saveRTMSblFailLogData(histid, setup_details, 'SBL6')

            except ZeroDivisionError:
                logging.info("No valid tested units for lot %s", setup_details.get('lotnum'))
                pass

            logging.info("Checking RTM setup %s for hardware data", setup_details.get('lotnum'))
            if setup_details.get('hardware_info') != None:
                self.saveRTMHardwareDetails(histid, setup_details.get('hardware_info'))
            else:
                logging.info("No hardware data for lot %s", setup_details.get('lotnum'))
    
    def saveRTMSiteVarianceFailLogData(self, histid, setup_details):
        with self.db_connections['rtm_db']["cred"].cursor() as cursor:
            site_yield = {}
            fail_logs = []
            for (sitenum, site_detail) in setup_details.get('bin_details').get('site_count').items():
                site_yield[sitenum] = site_detail.get('good', 0) / site_detail.get('total', 0) * 100

            upperbinpct = 0 if len(site_yield) < 1 else max(site_yield.values())
            upperbinsite = None if len(site_yield) < 1 else max(site_yield, key=site_yield.get)
            
            lowerbinpct = 0 if len(site_yield) < 1 else min(site_yield.values())
            lowerbinsites = [k for k,v in site_yield.items() if upperbinpct - v >= config.DEFAULT_SITEVAR]
            logging.info("Bin fall-out sites on lot %s detected at %s", setup_details.get('lotnum'), str(lowerbinsites))
            ismainmtf = 1
            for lowerbinsite in lowerbinsites:
                upperbincnt = setup_details['bin_details']['site_count'][upperbinsite]
                lowerbincnt = setup_details['bin_details']['site_count'][lowerbinsite]
                bins = [int(b) for b in list(set(list(upperbincnt.keys()) + list(lowerbincnt.keys()))) if b != "good" and b != "total"]
                bins.sort()
                bin_s2s = {}
                for bin in bins:
                    _bin = str(bin)
                    lbc = lowerbincnt[_bin] if _bin in lowerbincnt else 0
                    ubc = upperbincnt[_bin] if _bin in upperbincnt else 0
                    bin_s2s[_bin] = (lbc / lowerbincnt['total'] * 100) - (ubc / upperbincnt['total'] * 100)
                
                bin_mtf = None if len(bin_s2s) < 1 else max(bin_s2s, key=bin_s2s.get)
                flid = str(md5(str(histid + "_SITEVAR" + str(bin_mtf)).encode()).hexdigest())
                logging.info("Bin %s on site %s in lot %s captured. Main MTF = %s", bin_mtf, lowerbinsite, setup_details.get('lotnum'), ismainmtf)
                fail_logs.append(tuple((flid, histid, "SITEVAR", bin_mtf, lowerbinsite, ismainmtf)))
                ismainmtf = 0
                
            cursor.executemany(RTMQueries._RTMQueries__setRTMFailLogDetails(), fail_logs)
            self.db_connections['rtm_db']["cred"].commit()
            logging.info("Site Variance details for lot %s saved", setup_details.get('lotnum'))

    def saveRTMSylFailLogData(self, histid, setup_details):
        with self.db_connections['rtm_db']["cred"].cursor() as cursor:
            flid = str(md5(str(histid + "_SYL").encode()).hexdigest())
            fail_logs = tuple((flid, histid, "SYL", None, None, 1))
            cursor.execute(RTMQueries._RTMQueries__setRTMFailLogDetails(), fail_logs)
            self.db_connections['rtm_db']["cred"].commit()
            logging.info("SYL details for lot %s saved", setup_details.get('lotnum'))
        
    def saveRTMSblFailLogData(self, histid, setup_details, sbl_fail_type):
        with self.db_connections['rtm_db']["cred"].cursor() as cursor:
            sbl_yield = {}
            for (sitenum, site_detail) in setup_details.get('bin_details').get('site_count').items():
                sbl_yield[sitenum] = site_detail.get('5' if sbl_fail_type == 'SBL5' else '6', 0) / site_detail.get('total', 0) * 100
            flid = str(md5(str(histid + "_" + sbl_fail_type).encode()).hexdigest())
            lowerbinsite = None if len(sbl_yield) < 1 else max(sbl_yield, key=sbl_yield.get)
            fail_logs = tuple((flid, histid, sbl_fail_type, 5 if sbl_fail_type == "SBL5" else 6, lowerbinsite, 1))
            cursor.execute(RTMQueries._RTMQueries__setRTMFailLogDetails(), fail_logs)
            self.db_connections['rtm_db']["cred"].commit()
    
    def saveRTMSiteDetails(self, histid, bin_details):
        if not bin_details.get("site_count"):
            logging.info("No site details detected")
            return
        
        data = []
        with self.db_connections['rtm_db']["cred"].cursor() as cursor:
            for (site, bin_detail) in bin_details.get('site_count').items():
                for (bin, cnt) in bin_detail.items():
                    if bin not in ['good', 'total']:
                        data.append(tuple((histid, site, bin, cnt)))

            cursor.executemany(RTMQueries._RTMQueries__setRTMSetupSiteDetails(), data)
            self.db_connections['rtm_db']["cred"].commit()
            logging.info("Site details saved, with histid = %s", histid)
        return

    def saveRTMHardwareDetails(self, histid, hardware_details):
        data = []
        with self.db_connections['rtm_db']["cred"].cursor() as cursor:
            for (paramname, paramvalue) in hardware_details.items():
                if paramvalue != None:
                    data.append(tuple((histid, paramname, paramvalue)))
            
            cursor.executemany(RTMQueries._RTMQueries__setRTMSetupHardwareDetails(), data)
            self.db_connections['rtm_db']["cred"].commit()
            logging.info("Hardware details saved, with histid = %s", histid)
        return

    def __getCamstarLotDetails(self, lotnums : list):
        with self.db_connections['camstar_db']["cred"].cursor() as cursor:
            cursor.execute(RTMQueries._RTMQueries__getCamstarLotDetails2().format(str(tuple(lotnums)), str(tuple(lotnums))))
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            res = cursor.fetchall()
        return res

    def __getCamstarHardwareDetails(self, testerids : list):
        result = {}
        with self.db_connections['camstar_db']["cred"].cursor() as cursor:
            cursor.execute(RTMQueries._RTMQueries__getCamstarHWDetails().format(str(tuple(testerids))))
            for (paramname, paramvalue, ishardware, resourcename) in cursor:
                if resourcename not in list(result.keys()):
                    if resourcename in ['PROGRAMNAME', 'PROGRAMREV']: continue
                    result[resourcename] = {}
                if paramname not in list(result[resourcename].keys()):
                    result[resourcename][paramname] = paramvalue
        return result

    def __getCamstarLimits(self, devices : list):
        with self.db_connections['camstar_db']["cred"].cursor() as cursor:
            cursor.execute(RTMQueries._RTMQueries__getCamstarLimitDetails().format(str(tuple(devices))))
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            res = cursor.fetchall()
        return res

    def __getEquipmentHistory(self, testerid : str, lotnum : str, snipdate : str):
        with self.db_connections['bridge_db']['cred'].cursor() as cursor:
            cursor.execute(RTMQueries._RTMQueries__getEquipmentHistory(), (testerid, lotnum, snipdate))
            res = cursor.fetchall()
        return res