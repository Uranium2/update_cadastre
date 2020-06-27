import pymysql
import csv
import os
import datetime
import requests
import gzip
import csv
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime

dir_path = os.path.dirname(os.path.realpath(__file__))
foldername = os.path.basename(dir_path)
f = open(os.path.join(dir_path, "aws_keys"), "r")
keys = f.read().split("\n")

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

conn = pymysql.connect(
  host= keys[2],
  user=keys[3],
  password=keys[4],
  port=keys[5])

cursor = conn.cursor()

currentDirectory = os.path.dirname(os.path.realpath(__file__))
now = datetime.now()

url_cadastre = "https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{}/departements/".format(now.year - 1)
print(url_cadastre)

def get_date_cadastre():
    response = requests.get(url_cadastre, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    res = soup.find_all('a', href=True)
    line_date = str(res[2].next_sibling)
    date = line_date.split()[0]
    return date

def check_cadastre_update():
    file_path = os.path.join(currentDirectory, "last_date_import.txt")
    try:
        text_file = open(file_path)
    except :
        text_file = open(file_path, "w")
        text_file.close()

    text_file = open(file_path, "r")
    content = text_file.read()
    date = get_date_cadastre()
    text_file.close()
    if content != date:
        text_file = open(file_path, "w")
        text_file.write(date)
        text_file.close()
        return True
    else:
        print("No need to update")
        return False
    
if check_cadastre_update():
    print("DL file")
    filename = "75.csv.gz"
    r = requests.get(url_cadastre + filename, allow_redirects=True)

    local_csv = os.path.join(currentDirectory, filename)
    open(local_csv, 'wb').write(r.content)

    with gzip.open(local_csv, mode="rt", encoding='utf-8') as f:
        df = pd.read_csv(f)
        
        # Pre traitement
        df = df[["date_mutation", "valeur_fonciere", "code_type_local", "type_local", "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain", "longitude", "latitude"]]
        df = df.fillna(0)
        df = df.drop_duplicates()

        args = []
        cols = ",".join([str(i) for i in df.columns.tolist()])
        for i, row in df.iterrows():
            sql = "REPLACE INTO predimmo.data(" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))
            if i % 100 == 0:
                print("Running... at line " + str(i))
            conn.commit()
