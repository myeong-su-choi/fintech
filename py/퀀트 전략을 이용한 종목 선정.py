import pandas as pd
import numpy as np
import statsmodels.api as sm

from sqlalchemy import create_engine
from scipy.stats import zscore


# 밸류 포트폴리오 구하기
def value_portfolio():
    
    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    value_list = pd.read_sql("""
        select * from kor_value
        where 기준일 = (select max(기준일) from kor_value);
    """, con=engine)

    engine.dispose()

    value_list.loc[value_list['값'] <= 0, '값'] = np.nan
    value_pivot = value_list.pivot(index='종목코드', columns='지표', values='값')
    data_bind = ticker_list[['종목코드', '종목명']].merge(value_pivot,
                                                        how='left',
                                                        on='종목코드')
    
    value_list_copy = data_bind.copy()
    value_list_copy['DY'] = 1 / value_list_copy['DY']
    value_list_copy = value_list_copy[['PER', 'PBR', 'PCR', 'PSR', 'DY']]
    value_rank_all = value_list_copy.rank(axis=0)

    value_sum_all = value_rank_all.sum(axis=1, skipna=False).rank()

    return data_bind.loc[value_sum_all <= 20]


# 모멘텀 포트폴리오 구하기
def momentum_portfolio():
    
    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
    
    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    price_list = pd.read_sql("""
        select 날짜, 종가, 종목코드
        from kor_price
        where 날짜 >= (select (select max(날짜) from kor_price) - interval 1 year);
    """, con=engine)

    engine.dispose()

    price_pivot = price_list.pivot(index='날짜', columns='종목코드', values='종가')
    
    data_bind = ticker_list[['종목코드', '종목명']]

    # K-Ratio
    ret = price_pivot.pct_change().iloc[1:]
    ret_cum = np.log(1 + ret).cumsum()

    x = np.array(range(len(ret)))
    k_ratio = {}

    for i in range(0, len(ticker_list)):

        ticker = data_bind.loc[i, '종목코드']

        try:
            y = ret_cum.loc[:, price_pivot.columns == ticker]
            reg = sm.OLS(y, x).fit()
            res = float((reg.params / reg.bse).iloc[0])
        except:
            res = np.nan
        
        k_ratio[ticker] = res
    
    k_ratio_bind = pd.DataFrame.from_dict(k_ratio, orient='index').reset_index()
    k_ratio_bind.columns = ['종목코드', 'K_ratio']
    
    data_bind = data_bind.merge(k_ratio_bind, how='left', on='종목코드')
    k_ratio_rank = data_bind['K_ratio'].rank(axis=0, ascending=False)
    
    return data_bind[k_ratio_rank <= 20]


# 우량성 포트폴리오 구하기
def quality_portfolio():

    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    fs_list = pd.read_sql("""
        select * from kor_fs
        where 계정 in ('당기순이익', '매출총이익', '영업활동으로인한현금흐름', '자산', '자본')
        and 공시구분 = 'q';
    """, con=engine)

    fs_list = fs_list.sort_values(['종목코드', '계정', '기준일'])
    fs_list['ttm'] = fs_list.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(window=4, min_periods=4).sum()['값']
    fs_list_clean = fs_list.copy()
    fs_list_clean['ttm'] = np.where(fs_list_clean['계정'].isin(['자산', '자본']), fs_list_clean['ttm'] / 4, fs_list_clean['ttm'])
    fs_list_clean = fs_list_clean.groupby(['종목코드', '계정']).tail(1)

    fs_list_pivot = fs_list_clean.pivot(index='종목코드', columns='계정', values='ttm')
    fs_list_pivot['ROE'] = fs_list_pivot['당기순이익'] / fs_list_pivot['자본']
    fs_list_pivot['GPA'] = fs_list_pivot['매출총이익'] / fs_list_pivot['자산']
    fs_list_pivot['CFO'] = fs_list_pivot['영업활동으로인한현금흐름'] / fs_list_pivot['자산']

    quality_list = ticker_list[['종목코드', '종목명']].merge(fs_list_pivot,
                                                           how='left',
                                                           on='종목코드')
    
    quality_list_copy = quality_list[['ROE', 'GPA', 'CFO']].copy()
    quality_rank = quality_list_copy.rank(ascending=False, axis=0)

    quality_sum = quality_rank.sum(axis=1, skipna=False).rank()
    
    return quality_list.loc[quality_sum <= 20, ['종목코드', '종목명', 'ROE', 'GPA', 'CFO']].round(4)


# 마법공식 포트폴리오 구하기
def magic_formula_portfolio():

    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    fs_list = pd.read_sql("""
        select * from kor_fs
        where 계정 in ('매출액', '당기순이익', '법인세비용', '이자비용', '현금및현금성자산', '부채', '유동부채', '유동자산', '비유동자산', '감가상각비') 
        and 공시구분 = 'q';
    """, con=engine)

    engine.dispose()

    fs_list = fs_list.sort_values(['종목코드', '계정', '기준일'])
    fs_list['ttm'] = fs_list.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(window=4, min_periods=4).sum()['값']
    fs_list_clean = fs_list.copy()
    fs_list_clean['ttm'] = np.where(fs_list_clean['계정'].isin(['부채', '유동부채', '유동자산', '비유동자산']), fs_list_clean['ttm'] / 4, fs_list_clean['ttm'])

    fs_list_clean = fs_list_clean.groupby(['종목코드', '계정']).tail(1)
    fs_list_pivot = fs_list_clean.pivot(index='종목코드', columns='계정', values='ttm')

    data_bind = ticker_list[['종목코드', '종목명', '시가총액']].merge(fs_list_pivot,
                                                                   how='left',
                                                                   on='종목코드')
    data_bind['시가총액'] = data_bind['시가총액'] / 100000000

    # 분자(EBIT)
    magic_ebit = data_bind['당기순이익'] + data_bind['법인세비용'] + data_bind['이자비용']

    # 분모
    magic_cap = data_bind['시가총액']
    magic_debt = data_bind['부채']

    # 분모: 여유자금
    magic_excess_cash = data_bind['유동부채'] - data_bind['유동자산'] + data_bind['현금및현금성자산']
    magic_excess_cash[magic_excess_cash < 0] = 0
    magic_excess_cash_final = data_bind['현금및현금성자산'] - magic_excess_cash

    magic_ev = magic_cap + magic_debt - magic_excess_cash_final

    # 이익수익률
    magic_ey = magic_ebit / magic_ev

    # 투하자본 수익률
    magic_ic = (data_bind['유동자산'] - data_bind['유동부채']) + (data_bind['비유동자산'] - data_bind['감가상각비'])
    magic_roc = magic_ebit / magic_ic

    # 열 입력하기
    data_bind['이익 수익률'] = magic_ey
    data_bind['투하자본 수익률'] = magic_roc

    magic_rank = (magic_ey.rank(ascending=False, axis=0) + magic_roc.rank(ascending=False, axis=0)).rank(axis=0)
    
    return data_bind.loc[magic_rank <= 20, ['종목코드', '종목명', '이익 수익률', '투하자본 수익률']].round(4)


# 섹터 중립 포트폴리오 구하기
def sector_neutral_portfolio():

    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    sector_list = pd.read_sql("""
        select * from kor_sector;
    """, con=engine)

    price_list = pd.read_sql("""
        select 날짜, 종가, 종목코드
        from kor_price
        where 날짜 >= (select (select max(날짜) from kor_price) - interval 1 year);
    """, con=engine)

    engine.dispose()

    price_pivot = price_list.pivot(index='날짜', columns='종목코드', values='종가')
    ret_list = pd.DataFrame(data=(price_pivot.iloc[-1] / price_pivot.iloc[0]) - 1, columns=['return'])

    data_bind = ticker_list[['종목코드', '종목명']].merge(
        sector_list[['CMP_CD', 'SEC_NM_KOR']],
        how='left',
        left_on='종목코드',
        right_on='CMP_CD'
    ).merge(
        ret_list,
        how='left',
        on='종목코드'
    )

    data_bind.loc[data_bind['SEC_NM_KOR'].isnull(), 'SEC_NM_KOR'] = '기타'
    data_bind['z-score'] = data_bind.groupby('SEC_NM_KOR', dropna=False)['return'].transform(lambda x: zscore(x, nan_policy='omit'))
    data_bind['z-rank'] = data_bind['z-score'].rank(axis=0, ascending=False)

    return data_bind.loc[data_bind['z-rank'] <= 20]


# 멀티팩터 포트폴리오 구학
def multi_factor_portfolio():

    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')

    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    fs_list = pd.read_sql("""
        select * from kor_fs
        where 계정 in ('당기순이익', '매출총이익', '영업활동으로인한현금흐름', '자산', '자본')
        and 공시구분 = 'q';
    """, con=engine)

    value_list = pd.read_sql("""
        select * from kor_value
        where 기준일 = (select max(기준일) from kor_value);
    """, con=engine)

    price_list = pd.read_sql("""
        select 날짜, 종가, 종목코드
        from kor_price
        where 날짜 >= (select (select max(날짜) from kor_price) - interval 1 year);
    """, con=engine)

    sector_list = pd.read_sql("""
        select * from kor_sector;
    """, con=engine)

    engine.dispose()

    fs_list = fs_list.sort_values(['종목코드', '계정', '기준일'])
    fs_list['ttm'] = fs_list.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(window=4, min_periods=4).sum()['값']
    fs_list_clean = fs_list.copy()
    fs_list_clean['ttm'] = np.where(fs_list_clean['계정'].isin(['자산', '자본']), fs_list_clean['ttm'] / 4, fs_list_clean['ttm'])
    fs_list_clean = fs_list_clean.groupby(['종목코드', '계정']).tail(1)

    fs_list_pivot = fs_list_clean.pivot(index='종목코드', columns='계정', values='ttm')
    fs_list_pivot['ROE'] = fs_list_pivot['당기순이익'] / fs_list_pivot['자본']
    fs_list_pivot['GPA'] = fs_list_pivot['매출총이익'] / fs_list_pivot['자산']
    fs_list_pivot['CFO'] = fs_list_pivot['영업활동으로인한현금흐름'] / fs_list_pivot['자산']
 
    value_list.loc[value_list['값'] <= 0, '값'] = np.nan
    value_pivot = value_list.pivot(index='종목코드', columns='지표', values='값')

    price_pivot = price_list.pivot(index='날짜', columns='종목코드', values='종가')
    ret_list = pd.DataFrame(data=(price_pivot.iloc[-1] / price_pivot.iloc[0]) - 1, columns=['12M'])

    ret = price_pivot.pct_change().iloc[1:]
    ret_cum = np.log(1 + ret).cumsum()

    x = np.array(range(len(ret)))
    k_ratio = {}

    for i in range(0, len(ticker_list)):

        ticker = ticker_list.loc[i, '종목코드']

        try:
            y = ret_cum.loc[:, price_pivot.columns == ticker]
            reg = sm.OLS(y, x).fit()
            res = float((reg.params / reg.bse).iloc[0])
        except:
            res = np.nan

        k_ratio[ticker] = res

    k_ratio_bind = pd.DataFrame.from_dict(k_ratio, orient='index').reset_index()
    k_ratio_bind.columns = ['종목코드', 'K_ratio']

    data_bind = ticker_list[['종목코드', '종목명']].merge(
        sector_list[['CMP_CD', 'SEC_NM_KOR']],
        how='left',
        left_on='종목코드',
        right_on='CMP_CD').merge(
            fs_list_pivot[['ROE', 'GPA', 'CFO']], 
            how='left',
            on='종목코드').merge(
                value_pivot, 
                how='left',
                on='종목코드').merge(
                    ret_list, 
                    how='left',
                    on='종목코드').merge(
                        k_ratio_bind,
                        how='left',
                        on='종목코드')

    data_bind.loc[data_bind['SEC_NM_KOR'].isnull(), 'SEC_NM_KOR'] = '기타'
    data_bind = data_bind.drop(['CMP_CD'], axis=1)

    def col_clean(df, cutoff=0.01, asc=False):

        q_low = df.quantile(cutoff)
        q_hi = df.quantile(1 - cutoff)

        df_trim = df[(df > q_low) & (df < q_hi)]

        if asc == False:
            df_z_score = df_trim.rank(axis=0, ascending=False).apply(zscore, nan_policy='omit')
        if asc == True:
            df_z_score = df_trim.rank(axis=0, ascending=True).apply(zscore, nan_policy='omit')

        return df_z_score
    
    data_bind_group = data_bind.set_index(['종목코드', 'SEC_NM_KOR']).groupby('SEC_NM_KOR', as_index=False)

    z_quality = data_bind_group[['ROE', 'GPA', 'CFO']].apply(lambda x: col_clean(x, 0.01, False)).sum(axis=1, skipna=False).to_frame('z_quality')
    data_bind = data_bind.merge(z_quality, how='left', on=['종목코드', 'SEC_NM_KOR'])

    
    value_1 = data_bind_group[['PBR', 'PCR', 'PER','PSR']].apply(lambda x: col_clean(x, 0.01, True))
    value_2 = data_bind_group[['DY']].apply(lambda x: col_clean(x, 0.01, False))

    z_value = value_1.merge(value_2, on=['종목코드', 'SEC_NM_KOR']).sum(axis=1, skipna=False).to_frame('z_value')
    data_bind = data_bind.merge(z_value, how='left', on=['종목코드', 'SEC_NM_KOR'])

    z_momentum = data_bind_group[['12M', 'K_ratio']].apply(lambda x: col_clean(x, 0.01, False)).sum(axis=1, skipna=False).to_frame('z_momentum')
    data_bind = data_bind.merge(z_momentum, how='left', on=['종목코드', 'SEC_NM_KOR'])

    data_bind_final = data_bind[['종목코드', 'z_quality', 'z_value', 'z_momentum']].set_index('종목코드').apply(zscore, nan_policy='omit')
    data_bind_final.columns = ['quality', 'value', 'momentum']

    wts = [0.3, 0.3, 0.3]
    data_bind_final_sum = (data_bind_final * wts).sum(axis=1, skipna=False).to_frame()
    data_bind_final_sum.columns = ['qvm']
    port_qvm = data_bind.merge(data_bind_final_sum, on='종목코드')
    port_qvm['invest'] = np.where(port_qvm['qvm'].rank() <= 20, 'Y', 'N')

    port_qvm[port_qvm['invest'] == 'Y'].to_excel('model.xlsx', index=False)

    return port_qvm[port_qvm['invest'] == 'Y']


value_portfolio_df = value_portfolio()
momentum_portfolio_df = momentum_portfolio()
quality_portfolio_df = quality_portfolio()
magic_formula_portfolio_df = magic_formula_portfolio()
sector_neutral_portfolio_df = sector_neutral_portfolio()
multi_factor_portfolio_df = multi_factor_portfolio()