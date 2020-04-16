# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sqlalchemy
from datetime import date, datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

engine_to = sqlalchemy.create_engine("mssql+pymssql://gleb-pwc:changeme@srv-ax-test/pwc")

def calculate_price(df, no_competitors=False, deficit=False, pickup=False, trades_type=1, **kwargs):
    dopobjem = kwargs.get('dopobjem')
    artfruit = kwargs.get('artfruit') 
    heatseal = kwargs.get('heatseal')
    superpereborka = kwargs.get('superpereborka')
    sku = kwargs.get('sku')
    
    skidka_dopobjem = 0
    
    # Если передано скю, значит расчет для единичного скю
    if sku:
        df = df[df['НоменклатураКод']==sku]
        dopobjem = dopobjem * df['ЕдиницаХраненияОстатковВес'].values[0]
    
        if dopobjem:
            df['СредняяЦенаКГ'] = df['НедельнаяСуммаПродаж']/df['НедельныйОбъемПродаж']
            if 1.4*df['НедельныйОбъемПродаж'].values[0] < dopobjem:
                (p_uch, p1, p2, v1) = df[['УчетнаяЦенаКГ', 'СредняяЦенаКГ', 'РекомендованнаяЦена', 'НедельныйОбъемПродаж']].values[0]
                skidka_dopobjem = (p1 - p_uch) * (v1 + dopobjem) * 0.5 / p1 / dopobjem + p_uch/p1 + 1
            
    if not no_competitors:
        #Пока что заполним пропуски нулями, надо убрать, когда будут полные данные от ГФ
        df[['ЗатратыЛогистика', 'ЗатратыСклад']] = df[['ЗатратыЛогистика', 'ЗатратыСклад']].fillna(0)

        #Первая группа наценок
        df['РекомендованнаяЦена'] = df['УчетнаяЦена'] * df['БрендНаценка'] * df['Рентабельность'] * (1 - skidka_dopobjem/100)

        #Добавление фикса за логистику и упаковку
        df['РекомендованнаяЦена'] += df['ЗатратыСклад']
        if not pickup:
            df['РекомендованнаяЦена'] += df['ЗатратыЛогистика']

        #Вторая группа наценок - применяются к Новой Цене (после фиксированной наценки за логистику и упаковку)
        df['РекомендованнаяЦена'] = df['РекомендованнаяЦена'] * df['Экспедирование']
    else:
        df['РекомендованнаяЦена'] = df['РекомендованнаяЦена_']

    df['РекомендованнаяЦена'] = df['РекомендованнаяЦена'].round(2)
    return df


class MainForm:
    def __init__(self, client, start, finish):
        self.client = client
        self.start = start
        self.finish = finish
        self.df = pd.DataFrame()
        
    def get_data(self):
        # С учетной ценой
        sku_df = pd.read_sql(f"""select convert(int, s.НоменклатураКод) НоменклатураКод, 
        s.Номенклатура,
        round(avg(pc.Цена), 2) УчетнаяЦена,
        round(avg(pc.ЦенаЗаКГ), 2) УчетнаяЦенаКГ
        from PrimeCosts pc 
        join SKU s on s.НоменклатураКод=pc.НоменклатураКод
        where pc.Период between '{self.start}' and '{self.finish}'
        group by s.НоменклатураКод, s.Номенклатура""", con=engine_to)
        
        cs_filter = f"""cs. Статус=1 and cs.КонтрагентКод={self.client} and
        cs.НоменклатураКод in {tuple(sku_df['НоменклатураКод'])}"""
        
        self.df = pd.read_sql(f"""select 
        cs.НоменклатураКод, 
        cs.МаксимальнаяЦенаПродажиКГ*s.ЕдиницаХраненияОстатковВес РекомендованнаяЦена_, --без конкурентов
        (1 + c.Экспедирование * c.Экспедирование_W) Экспедирование,
        (1 + cs.НовинкаНаценка) НовинкаНаценка,
        (1 + s.БрендНаценка) БрендНаценка,
        (1 + p.Рентабельность) Рентабельность,
        НедельнаяСуммаПродаж, НедельныйОбъемПродаж,
        c.[ЗатратыЛогистика, руб/кг] * s.ЕдиницаХраненияОстатковВес ЗатратыЛогистика,
        s.ЕдиницаХраненияОстатковВес,
        s.ЗатратыСклад
        from ClientSKU cs 
        join Clients c on c.КонтрагентКод=cs.КонтрагентКод
        left join SKU s on s.НоменклатураКод=cs.НоменклатураКод
        left join Profitability p on p.НоменклатураКод = cs.НоменклатураКод  and
        p.Год=year('{self.start}') and
        p.Месяц=month('{self.start}')
        where 
        {cs_filter}
        """, con=engine_to).merge(sku_df)
    
    
    
    def get_recommended_prices(self, no_competitors=False, deficit=False, pickup=False, trades_type=1):
        self.no_competitors = no_competitors
        self.deficit = deficit
        self.pickup = pickup
        self.trades_type = trades_type
        self.df = calculate_price(self.df, no_competitors, deficit, pickup, trades_type)
        return self.df[['НоменклатураКод', 'Номенклатура', 'УчетнаяЦена', 'РекомендованнаяЦена']], self.df
    
    
    def get_recommended_price(self, sku, dopobjem=0, artfruit=False, heatseal=False, superpereborka=False):
        return calculate_price(self.df, self.no_competitors, self.deficit, self.pickup, self.trades_type, 
                               sku=sku, dopobjem=dopobjem, artfruit=artfruit, 
                               heatseal=heatseal, superpereborka=superpereborka)[['НоменклатураКод', 'Номенклатура', 'УчетнаяЦена', 'РекомендованнаяЦена']]
    
     
if __name__ == '__main__':
    mf = MainForm(5527, '2020-04-14', '2020-04-26')
    mf.get_data()
    result, df = mf.get_recommended_prices(no_competitors=False, pickup=True)
    mf.get_recommended_price(524, dopobjem=600)