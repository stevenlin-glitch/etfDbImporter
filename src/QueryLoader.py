import YamlLoader

class QueryLoader:
    def __init__(_self, confPath):
        conf = YamlLoader.load(confPath)
        _self.tradingDays = conf['tradingDays']
        _self.tradingWeekday = conf['tradingWeekday']

    def getTradingDays(_self, year, month):
        return _self.tradingDays.format(year, month)
    
    def getTradingWeekday(_self, year, month, weekday):
        return _self.tradingWeekday.format(year, month, weekday)