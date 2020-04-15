# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sqlalchemy
from datetime import date, datetime, timedelta

engine_to = sqlalchemy.create_engine("mssql+pymssql://gleb-pwc:changeme@srv-ax-test/pwc")

class MainForm:
    def __init__(self, client, trades_type, start, finish, competitors=True, deficit=False, pickup=False):
        self.client = client
        self.trades_type = trades_type
        self.start = start
        self.finish = finish
        self.competitors = competitors
        self.deficit = deficit
        self.pickup = pickup
        
        
    def get_sku_list(self):
        # С учетной ценой
        df = pd.read_sql(f"""select convert(int, s.НоменклатураКод) НоменклатураКод, 
        s.Номенклатура,
        round(avg(pc.Цена), 2) УчетнаяЦена,
        round(avg(pc.ЦенаЗаКГ), 2) УчетнаяЦенаКГ
        from PrimeCosts pc 
        join SKU s on s.НоменклатураКод=pc.НоменклатураКод
        where pc.Период between '{self.start}' and '{self.finish}'
        group by s.НоменклатураКод, s.Номенклатура""", con=engine_to)
        return df
    
    
    def get_recommended_prices(self, sku_df):
        cs_filter = f"""cs. Статус=1 and cs.КонтрагентКод={self.client} and
        cs.НоменклатураКод in {tuple(sku_df['НоменклатураКод'])}"""
        
        df = pd.DataFrame(pd.read_sql(f"""select 
        cs.НоменклатураКод, 
        cs.МаксимальнаяЦенаПродажиКГ*s.ЕдиницаХраненияОстатковВес РекомендованнаяЦена_, --без конкурентов
        (1 + c.Экспедирование * c.Экспедирование_W) Экспедирование,
        (1 + cs.НовинкаНаценка) НовинкаНаценка,
        (1 + s.БрендНаценка) БрендНаценка,
        (1 + p.Рентабельность) Рентабельность,
        НедельнаяСуммаПродаж, НедельныйОбъемПродаж,
        c.[ЗатратыЛогистика, руб/кг] * s.ЕдиницаХраненияОстатковВес ЗатратыЛогистика,
        s.ЗатратыСклад
        from ClientSKU cs 
        join Clients c on c.КонтрагентКод=cs.КонтрагентКод
        left join SKU s on s.НоменклатураКод=cs.НоменклатураКод
        left join Profitability p on p.НоменклатураКод = cs.НоменклатураКод  and
        p.Год=year('{self.start}') and
        p.Месяц=month('{self.start}')
        where 
        {cs_filter}
        """, con=engine_to))
        res = sku_df.merge(df)
        
        if self.competitors:
            #Пока что заполним пропуски нулями, надо убрать, когда будут полные данные от ГФ
            res[['ЗатратыЛогистика', 'ЗатратыСклад']] = res[['ЗатратыЛогистика', 'ЗатратыСклад']].fillna(0)
            
            #Первая группа наценок
            res['РекомендованнаяЦена'] = res['УчетнаяЦена'] * res['БрендНаценка'] * res['Рентабельность']
            
            #Добавление фикса за логистику и упаковку
            res['РекомендованнаяЦена'] += res['ЗатратыСклад']
            if not self.pickup:
                res['РекомендованнаяЦена'] += res['ЗатратыЛогистика']
            
            #Вторая группа наценок - применяются к Новой Цене (после фиксированной наценки за логистику и упаковку)
            res['РекомендованнаяЦена'] = res['РекомендованнаяЦена'] * res['Экспедирование']
        else:
            res['РекомендованнаяЦена'] = res['РекомендованнаяЦена_']
            
        res['РекомендованнаяЦена'] = res['РекомендованнаяЦена'].round(2)
        return res[['НоменклатураКод', 'Номенклатура', 'УчетнаяЦена', 'РекомендованнаяЦена']], res
    

    


#Скидка за дополнительный объем. Если допобъем больше 40% от среднего недельного, то скидка составляет 
#(P_2-P_1)/P_1   =((P_1-P_уч )* 〖(V〗_1+V_2)*0,5)/〖P_1 V〗_2 +  P_уч/P_1 -1 
# V_1=средний недельный объем торгов за последние 4 недели
# V_2=новый недельный объем торгов 
# P_1=средняя цена продажи за 1 кг за последние 4 недели
# P_2=новая цена продажи 
# P_уч=текущая  учетная цена на 1 кг для данной ценовой группы

# main_df - датафрейм с расчетными показателями (получаемый get_recommended_prices)
def get_sku(sku, main_df, objem=0, artfruit=False, heatseal=False, superpereborka=False):
    #тут будет такой же расчет рекцены, как и в главном классе, но для одной строки
    #с учетом тумблеров по товару
    pass
     
if __name__ == '__main__':
    mf = MainForm(5527, 1, '2020-04-14', '2020-04-26', competitors=True, pickup=True)
    skus = mf.get_sku_list()
    result, df = mf.get_recommended_prices(skus)
    get_sku(14392, df)