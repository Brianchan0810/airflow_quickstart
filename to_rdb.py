import pandas as pd
import pymysql
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd

engine = create_engine("mysql+pymysql://newuser:newuser@172.16.240.201/HTHK")
db_connection = engine.connect()

def insert_sheet_to_db(sheet_name):
    df0 = pd.read_excel('3HK make up dataset.xlsx', sheet_name=sheet_name)
    df0.to_sql(sheet_name, db_connection, index=False)
    return 'successfully inserted'

for sheet in ['MSP', 'V_POS_MASTER', 'V_CHURN_PIVOT_CVM', 'EASY_CALL_RESULT', 'CUSTOMER_COMPLAIN_CASE', 'MNP'
              , 'WEB BROWSING DATA']:
    insert_sheet_to_db(sheet)

mydb = mysql.connector.connect(
    host = "172.16.240.201",
    user = "newuser",
    password = "newuser",
    database = "HTHK"
)

cursor = mydb.cursor()
cursor.execute("Select * from MSP")
result = cursor.fetchall()

columns_name = [item[0] for item in cursor.description]
columns_name

df = pd.DataFrame(result, columns=columns_name)

basic_info = df[['AC', 'LAST_NAME', 'FIRST_NAME', 'HKID', 'SEX', 'AGE_BAND']]
basic_info['AGE_GRP'] = basic_info['AGE_BAND'].map({
    'band_01':'18-22', 'band_02':'22-30','band_03':'30-45','band_04':'45-60','band_05':'60 or above'})
basic_info.drop(columns=['AGE_BAND'], inplace=True)
basic_info
basic_info.to_dict('records')