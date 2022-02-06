import csv
import mysql.connector
import pandas as pd

cnx = mysql.connector.connect(user="root",
                              password="Nott1ngham",
                              host="localhost",
                              port=3306,
                              use_pure=True)

# Use thousands=',' because commas are included in the dataset and things break when values are inserted into the database
tyredata = pd.read_csv('./Car_Tyres_Dataset.csv', thousands=',')

if cnx.is_connected():
        cursor = cnx.cursor()
        cursor.execute("CREATE DATABASE tyres")
        cursor.execute("USE tyres")

        cursor.execute( "CREATE TABLE tyres(Brand VARCHAR(255), Model varchar(255), Submodel varchar(255),Tyre_Brand varchar(255),SerialNo VARCHAR(255),Type VARCHAR (255),LoadIndex TINYINT, Size CHAR(11),SellingPrice INTEGER ,OriginalPrice INTEGER, Rating FLOAT)")

for i,row in tyredata.iterrows():
            #here %S means string values
            sql = "INSERT INTO tyres.tyres VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            values = tuple(map(lambda a : None if pd.isna(a) else a, row))
# little bit of magic to insert a NULL when rating is not set
            cursor.execute(sql, values) 
            # the connection is not auto committed by default, so we must commit to save our changes
            cnx.commit()

cursor.close()
