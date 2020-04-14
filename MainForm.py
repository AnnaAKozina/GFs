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
        df = pd.DataFrame(pd.read_sql(f"""select s.НоменклатураКод, 
        s.Номенклатура,
        round(avg(pc.Цена), 2) УчетнаяЦена
        from PrimeCosts pc 
        join SKU s on s.НоменклатураКод=pc.НоменклатураКод
        where pc.Период between '{self.start}' and '{self.finish}'
        group by s.НоменклатураКод, s.Номенклатура""", con=engine_to))
        
        return df
    
    
    def get_recommended_prices(self, sku_df):
        if self.competitors:
            df = pd.DataFrame(pd.read_sql(f"""select cs.НоменклатураКод, 
            (1 + c.Экспедирование * c.Экспедирование_W) Экспедирование,
            (1 + cs.НовинкаНаценка) НовинкаНаценка,
            (1 + s.БрендНаценка) БрендНаценка,
            (1 + p.Рентабельность) Рентабельность
            from ClientSKU cs 
            join Clients c on c.КонтрагентКод=cs.КонтрагентКод
            join SKU s on s.НоменклатураКод=cs.НоменклатураКод
            left join Profitability p on p.НоменклатураКод = cs.НоменклатураКод
            where 
            cs. Статус=1 and cs.КонтрагентКод={self.client} and
            cs.НоменклатураКод in {tuple(sku_df['НоменклатураКод'])} and
            p.Год=year('{self.start}') and
            p.Месяц=month('{self.start}')
            """, con=engine_to))
            
            res = sku_df.merge(df)
            
            #Первая группа наценок
            res['РекомендованнаяЦена'] = res['УчетнаяЦена'] * res['БрендНаценка'] * res['Рентабельность']
            
            #Вторая группа наценок - применяются к Новой Цене (после фиксированной наценки за HeatSeal и суперпереборку)
            res['РекомендованнаяЦена'] = res['РекомендованнаяЦена'] * res['Экспедирование']
        else:
            df = pd.DataFrame(pd.read_sql(f"""select cs.НоменклатураКод, 
            cs.МаксимальнаяЦенаПродажи РекомендованнаяЦена
            from ClientSKU cs 
            where cs.КонтрагентКод={self.client} and
            cs.НоменклатураКод in {tuple(sku_df['НоменклатураКод'])}
            """, con=engine_to))
            res = sku_df.merge(df)
        res['РекомендованнаяЦена'] = res['РекомендованнаяЦена'].round(2)
        return res[['НоменклатураКод', 'Номенклатура', 'УчетнаяЦена', 'РекомендованнаяЦена']]
    
    
mf = MainForm(5527, 1, '2020-04-14', '2020-04-26', competitors=False)
skus = mf.get_sku_list()
result = mf.get_recommended_prices(skus)
