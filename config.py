class RTMConfig():
    """
        Contains all default values and configuration of the RTM Summarizer module
    """
    FACILITY = 'MPOC'
    
    EAGLE_SAMPLE_SIZE = 100
    LTXMX_SAMPLE_SIZE = 50
    MICROFLEX_SAMPLE_SIZE = 50

    GOOD_BIN_NUMBERS = ['1','2','3','4']

    MIN_LOT_CHAR_LENGTH = 7

    DEFAULT_SYL = 75
    DEFAULT_SBL5 = 1
    DEFAULT_SBL6 = 1
    DEFAULT_SBL12 = 0

    DEFAULT_SITEVAR = 15

    EXCLUDE_OPERCODE = ['Unknown', 'MANUALMODE']

    PRODUCTION_STATUS_REASON = ['PRODUCTION', 'DISABLED SITE', 'DISABLED SITE - HANDLER']