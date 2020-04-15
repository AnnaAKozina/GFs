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
        if self.competitors:
            df = pd.DataFrame(pd.read_sql(f"""select cs.НоменклатураКод, 
            (1 + c.Экспедирование * c.Экспедирование_W) Экспедирование,
            (1 + cs.НовинкаНаценка) НовинкаНаценка,
            (1 + s.БрендНаценка) БрендНаценка,
            (1 + p.Рентабельность) Рентабельность,
            НедельнаяСуммаПродаж, НедельныйОбъемПродаж
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
            
            #Первая группа наценок
            res['РекомендованнаяЦена'] = res['УчетнаяЦена'] * res['БрендНаценка'] * res['Рентабельность']
            
            #Вторая группа наценок - применяются к Новой Цене (после фиксированной наценки за HeatSeal и суперпереборку)
            res['РекомендованнаяЦена'] = res['РекомендованнаяЦена'] * res['Экспедирование']
        else:
            df = pd.DataFrame(pd.read_sql(f"""select cs.НоменклатураКод, 
            cs.МаксимальнаяЦенаПродажиКГ*s.ЕдиницаХраненияОстатковВес РекомендованнаяЦена,
            (1 + c.Экспедирование * c.Экспедирование_W) Экспедирование,
            (1 + cs.НовинкаНаценка) НовинкаНаценка,
            (1 + s.БрендНаценка) БрендНаценка,
            (1 + p.Рентабельность) Рентабельность,
            НедельнаяСуммаПродаж, НедельныйОбъемПродаж
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
def dopobjem(sku, objem, main_df):
    df = pd.read_sql(f"""select * 
    from ClientSKU cs 
    where НоменклатураКод = {sku}
    and Статус=1""", engine_to)
    main_df['СредняяЦенаКГ'] = main_df['НедельнаяСуммаПродаж']/main_df['НедельныйОбъемПродаж']
    res = main_df[main_df['НоменклатураКод']==sku]
    if 1.4*df['НедельныйОбъемПродаж'].values[0] < objem:
        
        #v2=objem
        (p_uch, p1, p2, v1) = res[['УчетнаяЦенаКГ', 'СредняяЦенаКГ', 'РекомендованнаяЦена', 'НедельныйОбъемПродаж']].values[0]
        skidka = (p1 - p_uch) * (v1 + objem) * 0.5 / p1 / objem + p_uch/p1 + 1
    else:
        skidka = 0
        
    #Первая группа наценок
    res['РекомендованнаяЦена'] = res['УчетнаяЦена'] * res['БрендНаценка'] * res['Рентабельность'] * (1 - skidka/100)

    #Вторая группа наценок - применяются к Новой Цене (после фиксированной наценки за HeatSeal и суперпереборку)
    res['РекомендованнаяЦена'] = res['РекомендованнаяЦена'] * res['Экспедирование']
    return res['РекомендованнаяЦена'].values[0]
     
if __name__ == '__main__':
    mf = MainForm(5527, 1, '2020-04-14', '2020-04-26', competitors=False)
    skus = mf.get_sku_list()
    result, df = mf.get_recommended_prices(skus)
    dopobjem(14392, 560, df)