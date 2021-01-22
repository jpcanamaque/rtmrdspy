import MySQLdb, MySQLdb.cursors, cx_Oracle, json, datetime

class Database:
    """Database connectivity class"""
    
    @staticmethod
    def dbConnect(db_key):
        db_cred = Database.__dbConnectionDetails()[db_key]
        db_type = db_cred['type']
        try:
            if db_type == "mysql":
                mydb = MySQLdb.connect(
                    host=db_cred["host"],
                    user=db_cred["user"],
                    passwd=db_cred["pword"],
                    db=db_cred["alias"],
                    cursorclass = MySQLdb.cursors.SSDictCursor
                )
            elif db_type == "oracle":
                mydb = cx_Oracle.connect(db_cred["user"], db_cred["pword"], db_cred["host"]+"/"+db_cred["alias"])
            return {"type" : db_type, "cred" : mydb, "result" : 1}
        except Exception as e:
            print(str(e))
            return {"type" : db_type, "result" : 0}

    @staticmethod
    def __convertData(o):
        if isinstance(o, datetime.datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(o, bytearray):
            return o.__str__()

    @staticmethod
    def __dbConnectionDetails():
        db_accts = {}

        db_accts['MPOC_RTM_DB'] = {
            "type" : "mysql",
            "host" :  "host",
            "user" : "user",
            "pword" : "pword",
            "alias" : "alias"
        }

        db_accts['MPOC_RTY_DB'] = {
            "type" : "mysql",
            "host" :  "host",
            "user" : "user",
            "pword" : "pword",
            "alias" : "alias"
        }

        db_accts['MIPT_RTY_DB'] = {
            "type" : "mysql",
            "host" :  "host",
            "user" : "user",
            "pword" : "password",
            "alias" : "schema"
        }

        db_accts['MPOC_BRIDGE_DB'] = {
            "type" : "mysql",
            "host" :  "host",
            "user" : "user",
            "pword" : "password",
            "alias" : "schema"
        }

        db_accts['MIPT_BRIDGE_DB'] = {
            "type" : "mysql",
            "host" :  "host",
            "user" : "user",
            "pword" : "password",
            "alias" : "schema"
        }

        db_accts['MPOC_CAMSTAR_DB'] = {
            "type" : "oracle",
            "host" :  "host",
            "user" : "user",
            "pword" : "password",
            "alias" : "schema"
        }

        db_accts['MIPT_CAMSTAR_DB'] = {
            "type" : "oracle",
            "host" :  "host",
            "user" : "user",
            "pword" : "password",
            "alias" : "schema"
        }
        return db_accts