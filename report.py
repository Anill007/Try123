import requests
import sqlite3


def get_index_from_tables(data):
    indexes = []
    conn = sqlite3.connect('report.db')
    cur = conn.cursor()
    result = cur.execute(
        ''' SELECT ID FROM YEARS WHERE YEARS.YEAR=? ''', (data['year'],))
    year_index = result.fetchone()
    indexes.append(year_index[0])

    result = cur.execute(
        ''' SELECT ID FROM COUNTRIES WHERE COUNTRIES.COUNTRY=? ''', (data['country'],))
    country_index = result.fetchone()
    indexes.append(country_index[0])

    result = cur.execute(
        ''' SELECT ID FROM PETROLEUM WHERE PETROLEUM.TYPE=? ''', (data['petroleum_product'],))
    country_index = result.fetchone()
    indexes.append(country_index[0])
    conn.close()

    return indexes


def store_sales(datas):
    indexes = []
    conn = sqlite3.connect('report.db')
    cur = conn.cursor()

    for data in datas:
        indexes = get_index_from_tables(data)
        indexes.append(data['sale'])
        result = cur.execute(
            ''' INSERT INTO TRANSACTIONS(YEAR, COUNTRY, PETROLEUM, SALES) VALUES(?,?,?,?) ''', indexes)
        conn.commit()
    conn.close()


def overall_sales_by_country():
    conn = sqlite3.connect('report.db')
    cur = conn.cursor()
    res = cur.execute("SELECT COUNTRY FROM COUNTRIES")
    res = res.fetchall()
    all_countries = []
    for c_list in res:
        all_countries.append(c_list[0])

    res = cur.execute("SELECT TYPE FROM PETROLEUM")
    res = res.fetchall()
    all_petroleum = []
    for p_list in res:
        all_petroleum.append(p_list[0])

    print("%-20s %-25s %s" % ("COUNTRY", "PETROLEUM", "TOTAL_SALES"))
    print("-"*60)
    for country in all_countries:
        for petroleum in all_petroleum:
            res = cur.execute(
                ''' SELECT SUM(TRANSACTIONS.SALES)
                    FROM ((TRANSACTIONS 
                    INNER JOIN COUNTRIES ON TRANSACTIONS.COUNTRY = COUNTRIES.ID)
                    INNER JOIN PETROLEUM ON TRANSACTIONS.PETROLEUM = PETROLEUM.ID)
                    WHERE COUNTRIES.COUNTRY = ? AND TRANSACTIONS.SALES > 0 AND PETROLEUM.TYPE = ?;''', [country, petroleum])
            res = res.fetchone()
            print("%-20s %-25s %s" % (country, petroleum, res[0]))
        print("-"*60)
    conn.close()


def get_max_sales_year():
    yearly_sales_sum = {}
    year_list = []
    petroleum_list = []
    conn = sqlite3.connect('report.db')
    cur = conn.cursor()
    cur.execute(''' SELECT YEAR FROM YEARS ''')
    res = cur.fetchall()
    for year in res:
        year_list.append(year[0])

    cur.execute(''' SELECT TYPE FROM PETROLEUM ''')
    res = cur.fetchall()
    for petroleum in res:
        petroleum_list.append(petroleum[0])

    print("%-30s %s" % ("PETROLEUM PRODUCT", "MINIMUM SALE YEAR"))
    print("-"*60)
    for petroleum in petroleum_list:
        for year in year_list:
            cur.execute(''' SELECT SUM(TRANSACTIONS.SALES)
                            FROM ((TRANSACTIONS
                            INNER JOIN PETROLEUM ON PETROLEUM.ID = TRANSACTIONS.PETROLEUM)
                            INNER JOIN YEARS ON YEARS.ID = TRANSACTIONS.YEAR)
                            WHERE TRANSACTIONS.SALES > 0 AND YEARS.YEAR = ? AND PETROLEUM.TYPE =  ?
                            ''', [year, petroleum, ])
            res = cur.fetchone()
            res = res[0]
            if res != None:
                yearly_sales_sum[f'{year}'] = res

        min_value = 0
        min_year = 0

        for i, key in enumerate(yearly_sales_sum):
            if i == 0 or min_value > yearly_sales_sum[key]:
                min_year = key
                min_value = yearly_sales_sum[key]

        print("%-30s %s" % (petroleum, min_year))

    conn.close()


def avg_of_two_years():
    years = []
    petroleums = []
    conn = sqlite3.connect('report.db')
    cur = conn.cursor()
    cur.execute(''' SELECT YEAR FROM YEARS ORDER BY YEAR ASC ''')
    results = cur.fetchall()
    for result in results:
        years.append(result[0])

    cur.execute(''' SELECT TYPE FROM PETROLEUM ''')
    results = cur.fetchall()
    for result in results:
        petroleums.append(result[0])

    print('*'*70)
    print("%-30s %s %25s " % ("Product", "Year", "Avg"))
    print('*'*70)
    for petroleum in petroleums:
        for i in range(0, len(years)-1, 2):
            year_one = years[i]
            year_two = years[i+1]

            cur.execute(''' SELECT AVG(TRANSACTIONS.SALES) 
                        FROM ((TRANSACTIONS
                        INNER JOIN PETROLEUM ON PETROLEUM.ID = TRANSACTIONS.PETROLEUM)
                        INNER JOIN YEARS ON YEARS.ID = TRANSACTIONS.YEAR)
                        WHERE TRANSACTIONS.SALES > 0 AND (YEARS.YEAR = ? OR YEARS.YEAR = ?) AND PETROLEUM.TYPE = ?''', (year_one, year_two, petroleum))
            res = cur.fetchone()
            res = round(res[0], 3)
            print("%-30s (%s-%s) %20s" %
                  (petroleum, year_one, year_two, res))
        print('-'*70)


URL = "https://raw.githubusercontent.com/younginnovations/internship-challenges/master/programming/petroleum-report/data.json"
data = requests.get(url=URL)
data = data.json()
# avg_of_two_years()
# print(data)
# overall_sales_by_country()
# print(store_sales(data))
# get_max_sales_year()


# two years average

# DB PART--------------------------------------------------------------------------
# conn = sqlite3.connect('report.db')
# cur = conn.cursor()

# conn.execute(''' CREATE TABLE TRANSACTIONS (ID INTEGER PRIMARY KEY AUTOINCREMENT,
#                                     YEAR INTEGER NOT NULL,
#                                     COUNTRY INTEGER NOT NULL,
#                                     PETROLEUM INTEGER NOT NULL,
#                                     SALES INTEGER NOT NULL ); ''')

# conn.execute(''' CREATE TABLE YEARS (ID INTEGER PRIMARY KEY AUTOINCREMENT,
#                                     YEAR INTEGER NOT NULL UNIQUE); ''')

# conn.execute(''' CREATE TABLE COUNTRIES (ID INTEGER PRIMARY KEY AUTOINCREMENT,
#                                     COUNTRY TEXT NOT NULL UNIQUE); ''')

# conn.execute(''' CREATE TABLE PETROLEUM (ID INTEGER PRIMARY KEY AUTOINCREMENT,
#                                     TYPE TEXT NOT NULL UNIQUE); ''')

# distinct_years = []
# distinct_petroleum_products = []
# distinct_country = []

# # print(data[0])

# for d in data:
#     if d['year'] not in distinct_years:
#         distinct_years.append(d['year'])
#         cur.execute(''' INSERT INTO YEARS(YEAR) VALUES(?) ''', (d['year'], ))

# for d in data:
#     if d['petroleum_product'] not in distinct_petroleum_products:
#         distinct_petroleum_products.append(d['petroleum_product'])
#         cur.execute(
#             ''' INSERT INTO PETROLEUM(TYPE) VALUES(?) ''', (d['petroleum_product'], ))

# for d in data:
#     if d['country'] not in distinct_country:
#         distinct_country.append(d['country'])
#         cur.execute(
#             ''' INSERT INTO COUNTRIES(COUNTRY) VALUES(?) ''', (d['country'], ))
# conn.commit()
# conn.close()
