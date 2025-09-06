import requests as rq
import re
import pandas as pd
import numpy as np
import pymysql
import time

from bs4 import BeautifulSoup
from io import BytesIO
from tqdm import tqdm
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from datetime import date


# 최근 영업일 기준 데이터 받기
url = 'https://finance.naver.com/sise/sise_deposit.nhn'
data = rq.get(url)
data_html = BeautifulSoup(data.content, features="html.parser")
parse_day = data_html.select_one(
    'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text

biz_day = re.findall('[0-9]+', parse_day)
biz_day = ''.join(biz_day)


# kor_ticker 테이블 만들기
def create_kor_ticker_table():

    # 한국거래소의 업종분류 현황 크롤링
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_stk = {
        'mktId': 'STK', # 코스피 
        'trdDd': biz_day,
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
    }

    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
    }
    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
    sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')

    gen_otp_ksq = {
        'mktId': 'KSQ', # 코스닥 입력
        'trdDd': biz_day,
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
    }
    otp_ksq = rq.post(gen_otp_url, gen_otp_ksq, headers=headers).text

    down_sector_ksq = rq.post(down_url, {'code': otp_ksq}, headers=headers)
    sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding='EUC-KR')

    krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop=True)
    krx_sector['종목명'] = krx_sector['종목명'].str.strip()
    krx_sector['기준일'] = biz_day


    # 개별종목 지표 크롤링
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_data = {
        'searchType': '1',
        'mktId': 'ALL',
        'trdDd': biz_day,
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'
    }
    otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    krx_ind = rq.post(down_url, {'code': otp}, headers=headers)

    krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
    krx_ind['종목명'] = krx_ind['종목명'].str.strip()
    krx_ind['기준일'] = biz_day


    # 데이터 정리하기
    diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))

    kor_ticker = pd.merge(
        krx_sector,
        krx_ind,
        on=krx_sector.columns.intersection(krx_ind.columns).tolist(),
        how='outer'
    )

    kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                            np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',
                            np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',
                            np.where(kor_ticker['종목명'].isin(diff), '기타',
                            '보통주'))))
    kor_ticker = kor_ticker.reset_index(drop=True)
    kor_ticker.columns = kor_ticker.columns.str.replace(' ', '')
    kor_ticker = kor_ticker[['종목코드', '종목명', '시장구분', '종가', '시가총액', '기준일', 'EPS', '선행EPS', 'BPS', '주당배당금', '종목구분']]
    kor_ticker = kor_ticker.replace({np.nan: None})
    kor_ticker['기준일'] = pd.to_datetime(kor_ticker['기준일'])

    """
        create table kor_ticker
        (
            종목코드 varchar(6) not null,
            종목명 varchar(20),
            시장구분 varchar(6),
            종가 float,
            시가총액 float,
            기준일 date,
            EPS float,
            선행EPS float,
            BPS float,
            주당배당금 float,
            종목구분 varchar(5),
            primary key(종목코드)
        );
    """
    con = pymysql.connect(user='root',
                          passwd='1234',
                          host='127.0.0.1',
                          db='stock_db',
                          charset='utf8')
    
    mycursor = con.cursor()
    query = f"""
        insert into kor_ticker (종목코드, 종목명, 시장구분, 종가, 시가총액, 기준일, EPS, 선행EPS, BPS, 주당배당금, 종목구분)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) as new
        on duplicate key update
        종목명=new.종목명, 시장구분=new.시장구분, 종가=new.종가, 시가총액=new.시가총액, 기준일=new.기준일,
        EPS=new.EPS, 선행EPS=new.선행EPS, BPS=new.BPS, 주당배당금=new.주당배당금, 종목구분=new.종목구분;
    """

    args = kor_ticker.values.tolist()

    mycursor.executemany(query, args)
    con.commit()

    con.close()


# kor_sector 테이블 만들기
def create_kor_sector_table():

    # WICS 기준 섹터정보 크롤링
    sector_code = [
        'G25', 'G35', 'G50', 'G40', 'G10', 'G20', 'G55', 'G30', 'G15', 'G45'
    ]

    data_sector = []

    for i in tqdm(sector_code):
        url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd={i}'''
        data = rq.get(url).json()
        data_pd = pd.json_normalize(data['list'])

        data_sector.append(data_pd)

        time.sleep(2)
    
    kor_sector = pd.concat(data_sector, axis=0)
    kor_sector = kor_sector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR']]
    kor_sector['기준일'] = biz_day
    kor_sector['기준일'] = pd.to_datetime(kor_sector['기준일'])

    """
        create table kor_sector
        (
            IDX_CD varchar(3),
            CMP_CD varchar(6),
            CMP_KOR varchar(20),
            SEC_NM_KOR varchar(10),
            기준일 date,
            primary key(CMP_CD)
        );
    """
    con = pymysql.connect(user='root',
                          passwd='1234',
                          host='127.0.0.1',
                          db='stock_db',
                          charset='utf8')

    mycursor = con.cursor()
    query = f"""
        insert into kor_sector (IDX_CD, CMP_CD, CMP_KOR, SEC_NM_KOR, 기준일)
        values (%s, %s, %s, %s, %s) as new
        on duplicate key update
        IDX_CD = new.IDX_CD, CMP_KOR = new.CMP_KOR, SEC_NM_KOR = new.SEC_NM_KOR, 기준일 = new.기준일
    """

    args = kor_sector.values.tolist()

    mycursor.executemany(query, args)
    con.commit()

    con.close()


# kor_price 테이블 만들기
def create_kor_price_table(years_ago=0, months_ago=0, days_ago=0):

    # 전 종목 주가 크롤링
    """
        create table kor_price
        (
            날짜 date,
            시가 double,
            고가 double,
            저가 double,
            종가 double,
            거래량 double,
            종목코드 varchar(6),
            primary key(날짜, 종목코드)
        );
    """

    # DB 연결
    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
    con = pymysql.connect(user='root',
                        passwd='1234',
                        host='127.0.0.1',
                        db='stock_db',
                        charset='utf8')
    mycursor = con.cursor()

    # 티커리스트 불러오기
    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    # DB 저장 쿼리
    query = """
       insert into kor_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드)
       values (%s, %s, %s, %s, %s, %s, %s) as new
       on duplicate key update
       시가 = new.시가, 고가 = new.고가, 저가 = new.저가,
       종가 = new.종가, 거래량 = new.거래량; 
    """

    # 오료 발생시 저장할 리스트 생성
    error_list = []

    # 전종목 주가 다운로드 및 저장
    for i in tqdm(range(0, len(ticker_list))):

        # 티커 선택
        ticker = ticker_list['종목코드'][i]

        # 시작일과 종료일
        fr = (date.today() + relativedelta(years=years_ago, months=months_ago, days=days_ago)).strftime("%Y%m%d")
        to = (date.today()).strftime("%Y%m%d")

        # 오류 발생 시 이를 무시하고 다음 루프로 진행
        try:

            # url 생성
            url = f'''https://fchart.stock.naver.com/siseJson.nhn?symbol={ticker}&requestType=1&startTime={fr}&endTime={to}&timeframe=day'''

            # 데이터 다운로드
            data = rq.get(url).content
            data_price = pd.read_csv(BytesIO(data))

            # 데이터 클렌징
            price = data_price.iloc[:, 0:6]
            price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
            price = price.dropna()
            price['날짜'] = price['날짜'].str.extract('(\d+)')
            price['날짜'] = pd.to_datetime(price['날짜'])
            price['종목코드'] = ticker

            # 주가 데이터를 DB에 저장
            args = price.values.tolist()
            mycursor.executemany(query, args)
            con.commit()

        except:

            # 오류 발생시 error_list에 티커 저장하고 넘어가기
            print(ticker)
            error_list.append(ticker)

        # 타임슬립 적용
        time.sleep(2)

    # DB 연결 종료
    engine.dispose()
    con.close()


# kor_fs 테이블 만들기
def create_kor_fs_table():

    """
        create table kor_fs
        (
            계정 varchar(30),
            기준일 date,
            값 float,
            종목코드 varchar(6),
            공시구분 varchar(1),
            primary key(계정, 기준일, 종목코드, 공시구분)
        )
    """

    # DB 연결
    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
    con = pymysql.connect(user='root',
                          passwd='1234',
                          host='127.0.0.1',
                          db='stock_db',
                          charset='utf8')
    mycursor = con.cursor()

    # 티커리스트 불러오기
    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    # DB 저장 쿼리
    query = """
        insert into kor_fs (계정, 기준일, 값, 종목코드, 공시구분)
        values (%s, %s, %s, %s, %s) as new
        on duplicate key update
        값=new.값
    """

    # 오류 발생시 저장할 리스트 생성
    error_list = []


    # 재무제표 클렌징 함수
    def clean_fs(df, ticker, frequency):

        df = df[~df.loc[:, ~df.columns.isin(['계정'])].isna().all(axis=1)]
        df = df.drop_duplicates(['계정'], keep='first')
        df = pd.melt(df, id_vars='계정', var_name='기준일', value_name='값')
        df = df[~pd.isnull(df['값'])]
        df['계정'] = df['계정'].replace({'계산에 참여한 계정 펼치기': ''}, regex=True)
        df['기준일'] = pd.to_datetime(df['기준일'], format='%Y/%m') + pd.tseries.offsets.MonthEnd()
        df['종목코드'] = ticker
        df['공시구분'] = frequency

        return df


    # for loop
    for i in tqdm(range(0, len(ticker_list))):

        # 티커 선택
        ticker = ticker_list['종목코드'][i]

        # 오류 발생 시 이를 무시하고 다음 루프로 진행
        try:

            # url 생성
            url = f'http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A{ticker}'

            # 데이터 받아오기
            data = pd.read_html(url, displayed_only=False)

            # 연간 데이터
            data_fs_y = pd.concat([data[0].iloc[:, ~data[0].columns.str.contains('전년동기')], data[2], data[4]])
            data_fs_y = data_fs_y.rename(columns={data_fs_y.columns[0]: "계정"})

            # 결산년 찾기
            page_data = rq.get(url)
            page_data_html = BeautifulSoup(page_data.content, 'html.parser')

            fiscal_data = page_data_html.select('div.corp_group1 > h2')
            fiscal_data_text = fiscal_data[1].text
            fiscal_data_text = re.findall('[0-9]+', fiscal_data_text)

            # 결산년에 해당하는 계정만 남기기
            data_fs_y = data_fs_y.loc[:, (data_fs_y.columns == '계정') | (data_fs_y.columns.str[-2:].isin(fiscal_data_text))]

            # 클렌징
            data_fs_y_clean = clean_fs(data_fs_y, ticker, 'y')

            # 분기 데이터
            data_fs_q = pd.concat([data[1].iloc[:, ~data[1].columns.str.contains('전년동기')], data[3], data[5]])
            data_fs_q = data_fs_q.rename(columns={data_fs_q.columns[0]: "계정"})

            data_fs_q_clean = clean_fs(data_fs_q, ticker, 'q')

            # 두개 합치기
            data_fs_bind = pd.concat([data_fs_y_clean, data_fs_q_clean])

            # 재무제표 데이터를 DB에 저장
            args = data_fs_bind.values.tolist()
            mycursor.executemany(query, args)
            con.commit()

        except Exception as e:
        
            # 오류 발생시 해당 종목명을 저장하고 다음 루프로 이동
            print(ticker)
            error_list.append(ticker)

        # 타임슬립 적용
        time.sleep(2)

    # DB 연결 종료
    engine.dispose()
    con.close()


# kor_value 테이블 만들기
def create_kor_value_table():

    """
        create table kor_value
        (
            종목코드 varchar(6),
            기준일 date,
            지표 varchar(3),
            값 double,
            primary key (종목코드, 기준일, 지표)
        );
    """

    # DB 연결
    engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/stock_db')
    con = pymysql.connect(user='root',
                          passwd='1234',
                          host='127.0.0.1',
                          db='stock_db',
                          charset='utf8')
    mycursor = con.cursor()

    # 분기 재무제표 불러오기
    kor_fs = pd.read_sql("""
        select * from kor_fs
        where 공시구분 = 'q'
        and 계정 in ('당기순이익', '자본', '영업활동으로인한현금흐름', '매출액');
    """, con=engine)

    # 티커 리스트 불러오기
    ticker_list = pd.read_sql("""
        select * from kor_ticker
        where 종목구분 = '보통주';
    """, con=engine)

    engine.dispose()
    
    # TTM 구하기
    kor_fs = kor_fs.sort_values(['종목코드', '계정', '기준일'])
    kor_fs['ttm'] = kor_fs.groupby(['종목코드', '계정'], as_index=False)['값'].rolling(window=4, min_periods=4).sum()['값']

    # 자본은 평균 구하기
    kor_fs['ttm'] = np.where(kor_fs['계정'] == '자본', kor_fs['ttm'] / 4, kor_fs['ttm'])
    kor_fs = kor_fs.groupby(['계정', '종목코드']).tail(1)

    kor_fs_merge = kor_fs[['계정', '종목코드', 'ttm']].merge(ticker_list[['종목코드', '시가총액', '기준일']], on='종목코드')
    kor_fs_merge['시가총액'] = kor_fs_merge['시가총액'] / 100000000

    kor_fs_merge['value'] = kor_fs_merge['시가총액'] / kor_fs_merge['ttm']
    kor_fs_merge['value'] = kor_fs_merge['value'].round(4)
    kor_fs_merge['지표'] = np.where(kor_fs_merge['계정'] == '매출액', 'PSR', 
                          np.where(kor_fs_merge['계정'] == '영업활동으로인한현금흐름', 'PCR',
                          np.where(kor_fs_merge['계정'] == '자본', 'PBR',
                          np.where(kor_fs_merge['계정'] == '당기순이익', 'PER', 
                          None))))

    kor_fs_merge.rename(columns={'value': '값'}, inplace=True)
    kor_fs_merge = kor_fs_merge[['종목코드', '기준일', '지표', '값']]
    kor_fs_merge = kor_fs_merge.replace([np.inf, -np.inf, np.nan], None)

    
    query = """
        insert into kor_value (종목코드, 기준일, 지표, 값)
        values (%s, %s, %s, %s) as new
        on duplicate key update
        값=new.값
    """

    args_fs = kor_fs_merge.values.tolist()
    mycursor.executemany(query, args_fs)
    con.commit()
    
    ticker_list['값'] = ticker_list['주당배당금'] / ticker_list['종가']
    ticker_list['값'] = ticker_list['값'].round(4)
    ticker_list['지표'] = 'DY'
    dy_list = ticker_list[['종목코드', '기준일', '지표', '값']]
    dy_list = dy_list.replace([np.inf, -np.inf, np.nan], None)
    dy_list = dy_list[dy_list['값'] != 0]

    args_dy = dy_list.values.tolist()
    mycursor.executemany(query, args_dy)
    con.commit()

    engine.dispose()
    con.close()

create_kor_ticker_table()
create_kor_sector_table()
create_kor_price_table(years_ago=-5)
create_kor_fs_table()
create_kor_value_table()
