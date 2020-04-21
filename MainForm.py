# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sqlalchemy
from datetime import date, datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

engine_to = sqlalchemy.create_engine("mssql+pymssql://gleb-pwc:changeme@srv-ax-test/pwc")

def calculate_price(df, no_competitors=False, deficit=False, pickup=False, trades_type=1, **kwargs):
    if df.shape[0] == 0:
        return pd.DataFrame(columns=['ЦеноваяГруппаКод', 'НоменклатураКод', 'РекомендованнаяЦена_',
       'Экспедирование', 'НовинкаНаценка', 'НовинкаС', 'БрендНаценка',
       'Рентабельность', 'НедельнаяСуммаПродаж', 'НедельныйОбъемПродаж',
       'ЗатратыЛогистика', 'ЕдиницаХраненияОстатковВес', 'ЗатратыСклад',
       'ЦенаТекущихТоргов', 'Бонус', 'Штраф', 'СостояниеСтокаГФ',
       'Ситуация_на_рынке', 'ИзменениеЦены', 'СкидкаПриОверстоке',
       'Номенклатура', 'УчетнаяЦена', 'УчетнаяЦенаКГ', 'РекомендованнаяЦена'])
        
    dopobjem = kwargs.get('dopobjem')
    artfruit = kwargs.get('artfruit') 
    heatseal = kwargs.get('heatseal')
    superpereborka = kwargs.get('superpereborka')
    sku = kwargs.get('sku')
    toolow = kwargs.get('toolow')
    toohigh = kwargs.get('toohigh')
    toolowvalue = kwargs.get('toolowvalue')
    toohighvalue = kwargs.get('toohighvalue')
    novinka = kwargs.get('novinka')
    start = kwargs.get('start')
    
    skidka_dopobjem = 0
    
    if sku:
        df = df[df['НоменклатураКод']==sku]
        dopobjem = dopobjem * df['ЕдиницаХраненияОстатковВес'].values[0]
    
        if dopobjem:
            df['СредняяЦенаКГ'] = df['НедельнаяСуммаПродаж']/df['НедельныйОбъемПродаж']
            if 1.4*df['НедельныйОбъемПродаж'].values[0] < dopobjem:
                (p_uch, p1, p2, v1) = df[['УчетнаяЦенаКГ', 'СредняяЦенаКГ', 'РекомендованнаяЦена', 'НедельныйОбъемПродаж']].values[0]
                skidka_dopobjem = (p1 - p_uch) * (v1 + dopobjem) * 0.5 / p1 / dopobjem + p_uch/p1 + 1
                
        # Если в интерфейсе системы для выбранного SKU пользователь проставляет галочку в поле «Слишком низкая учетная цена», 
        # необходимо использовать значение из поля <РазмерСкидки> источника №8, соответствующему значениям «Дефицит» 
        # и «Дефицит на 1-2 недели» в первых двух столбцах таблицы, соответственно
        # Если в интерфейсе системы для выбранного SKU пользователь проставляет галочку в поле «Слишком высокая учетная цена», 
        # необходимо использовать значение из поля <РазмерСкидки> источника №8, соответствующему значениям «Оверсток» и 
        # «Оверсток от 150% до 200% стандартного запаса» в первых двух столбцах таблицы, соответственно 
        if toolow:
            df['ИзменениеЦены'] = toolowvalue
        if toohigh:
            df['ИзменениеЦены'] = toohighvalue
            
        
            
    if not no_competitors:
        #Первая группа наценок
        df['РекомендованнаяЦена'] = df['УчетнаяЦена'] * df['БрендНаценка'] * df['Рентабельность'] * (1 - skidka_dopobjem/100)

        #Добавление фикса за логистику и упаковку
        df['РекомендованнаяЦена'] += df['ЗатратыСклад']
        if not pickup:
            df['РекомендованнаяЦена'] += df['ЗатратыЛогистика']
        
        #Новинка
        if novinka:
            if df['НовинкаС'].iloc[0] <= pd.to_datetime(start):
                df['РекомендованнаяЦена'] = df['РекомендованнаяЦена'] * df['НовинкаНаценка']

        #Вторая группа наценок - применяются к Новой Цене (после фиксированной наценки за логистику и упаковку)
        df['РекомендованнаяЦена'] = df['РекомендованнаяЦена'] * df['Экспедирование']
        df['РекомендованнаяЦена'] = df['РекомендованнаяЦена'] * df['Штраф']
        #Дефицит-Оверсток
        #Скидка по оверстоку применяется только для клиентов <КонтрагентКод>, для которых разрешается ее применение
        if not df.iloc[0]['СкидкаПриОверстоке']:
            df.loc[df['ИзменениеЦены']<0,'ИзменениеЦены'] = 0
        df['РекомендованнаяЦена'] = df['РекомендованнаяЦена'] * (1 + df['ИзменениеЦены'])
        
        #Дефицитные торги
        if deficit:
            df['РекомендованнаяЦена'] = df.apply(lambda x: x['РекомендованнаяЦена'] if pd.isnull(x['ЦенаТекущихТоргов'])
                                                                                     else x['ЦенаТекущихТоргов'], axis=1)
            
        # В самом конце Бонусы и премии
        df['РекомендованнаяЦена'] = df['РекомендованнаяЦена']*df['Бонус']
        
        # Если рекомендованная цена продажи с учетом скидки за оверсток ниже учетной цены, рекомендованная цена = учетной цене
        if df.iloc[0]['СкидкаПриОверстоке']:
            df.loc[(df['ИзменениеЦены']<0)&(df['РекомендованнаяЦена']<df['УчетнаяЦена']),'РекомендованнаяЦена'] = \
            df.loc[(df['ИзменениеЦены']<0)&(df['РекомендованнаяЦена']<df['УчетнаяЦена']),'УчетнаяЦена']
        
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
        self.toolowvalue = pd.read_sql("""select [Изменение_рекомендованной_цены_продажи] 
from skidki_overstok
where [Описание_ситуации_на_рынке]='Дефицит'
and [Описание_состояния_стока_ГФ]='Дефицит на 1-2 недели'""", engine_to).iloc[0]['Изменение_рекомендованной_цены_продажи']
        self.toohighvalue = pd.read_sql("""select [Изменение_рекомендованной_цены_продажи] 
from skidki_overstok
where [Описание_ситуации_на_рынке]='Оверсток'
and [Описание_состояния_стока_ГФ]='Оверсток от одного лишнего дня до 200% стандартного запаса'""", engine_to).iloc[0]['Изменение_рекомендованной_цены_продажи']
        
    def get_data(self):
        # !!!!! Узнать !!!!!
        VAT = 0.2
        
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
        s.ЦеноваяГруппаКод,
        cs.НоменклатураКод, 
        cs.МаксимальнаяЦенаПродажиКГ*s.ЕдиницаХраненияОстатковВес РекомендованнаяЦена_, --без конкурентов
        (1 + c.Экспедирование * c.Экспедирование_W) Экспедирование,
        (1 + cs.НовинкаНаценка) НовинкаНаценка,
        cs.НовинкаС,
        (1 + s.БрендНаценка) БрендНаценка,
        (1 + p.Рентабельность) Рентабельность,
        НедельнаяСуммаПродаж, НедельныйОбъемПродаж,
        c.[ЗатратыЛогистика, руб/кг] * s.ЕдиницаХраненияОстатковВес ЗатратыЛогистика,
        s.ЕдиницаХраненияОстатковВес,
        s.ЗатратыСклад,
        1.05 * cs.ЦенаТекущихТорговЗаКг * s.ЕдиницаХраненияОстатковВес ЦенаТекущихТоргов,
        (1 + (({1 - VAT}) * c.Бонус + c.Премия) * c.Бонус_W) Бонус,
        (1 + c.Штраф) Штраф,
        dov.СостояниеСтокаГФ, dov.Ситуация_на_рынке, dov.ИзменениеЦены,
        c.СкидкаПриОверстоке
        from ClientSKU cs 
        join Clients c on c.КонтрагентКод=cs.КонтрагентКод
        left join SKU s on s.НоменклатураКод=cs.НоменклатураКод
        left join DeficitOverstock dov on dov.ЦеноваяГруппаКод=s.ЦеноваяГруппаКод
        and dov.Дата='{self.start}'
        left join Profitability p on p.НоменклатураКод = cs.НоменклатураКод  and
        p.Год=year('{self.start}') and
        p.Месяц=month('{self.start}')
        where 
        {cs_filter}
        """, con=engine_to).merge(sku_df)
        #Пока что заполним пропуски нулями, надо убрать, когда будут полные данные от ГФ
        self.df[['ЗатратыЛогистика', 'ЗатратыСклад']] = self.df[['ЗатратыЛогистика', 'ЗатратыСклад']].fillna(0)
    
    
    def get_recommended_prices(self, no_competitors=False, deficit=False, pickup=False, trades_type=1):
        self.no_competitors = no_competitors
        self.deficit = deficit
        self.pickup = pickup
        self.trades_type = trades_type
        self.df = calculate_price(self.df, no_competitors, deficit, pickup, trades_type)
        return self.df[['НоменклатураКод', 'Номенклатура', 'УчетнаяЦена', 'РекомендованнаяЦена']], self.df
    
    
    def get_recommended_price(self, sku, start, toolow=False, toohigh=False, dopobjem=0, artfruit=False, 
                              heatseal=False, superpereborka=False, novinka=False):
        return calculate_price(self.df, self.no_competitors, self.deficit, self.pickup, self.trades_type, 
                               sku=sku, toolow=toolow, toohigh=toohigh, dopobjem=dopobjem, artfruit=artfruit, 
                               heatseal=heatseal, superpereborka=superpereborka, novinka=novinka, start=start,
                               toolowvalue=self.toolowvalue, toohighvalue=self.toohighvalue)[['НоменклатураКод', 'Номенклатура',
                                                                'УчетнаяЦена', 'РекомендованнаяЦена']]
    
     
if __name__ == '__main__':
    mf = MainForm(10763, '2020-04-21', '2020-04-26')
    mf.get_data()
    result, df = mf.get_recommended_prices(no_competitors=False, pickup=True)
    mf.get_recommended_price(13111, '2020-04-21', dopobjem=6, toolow=False, novinka=True)