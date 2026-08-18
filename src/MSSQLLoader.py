import pymssql
import YamlLoader
class MSSQLDB:
    CONF_HOST = "host"
    CONF_USER = "user"
    CONF_PWD = "pass"

    def __init__(_self, dbConf):
        _self.conf = YamlLoader.load(dbConf)

    def __del__(_self):
        _self.close()

    def open(_self):
        _self.close()
        cfg = _self.conf.get('pymssql', _self.conf)
        _self.conn = pymssql.connect(
                host = cfg[MSSQLDB.CONF_HOST],
                user = cfg[MSSQLDB.CONF_USER],
                password = cfg[MSSQLDB.CONF_PWD],
                charset = 'UTF-8')

    def close(_self):
        if (hasattr(_self, 'conn')):
            _self.conn.close()
            del _self.conn

    def exec(_self, sql):
        ret = []
        with _self.conn.cursor(as_dict=True) as cursor:
            cursor.execute(sql)
            for row in cursor:
                ret.append(row)

        return ret

