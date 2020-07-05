import os
import csv
import gzip
import pymysql
import datetime
import requests
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
  port=int(keys[5]))

cursor = conn.cursor()

currentDirectory = os.path.dirname(os.path.realpath(__file__))
now = datetime.now()


def get_url(year):
    """ Get url's dataset from datagouv.fr

    Args:
        year [Int]: Get url dataset from specify year.

    Returns:
        url_cadastre [String]: Return cadastre URL.
    """
    url_cadastre = "https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/{}/departements/".format(year)
    print(url_cadastre)
    return url_cadastre


def get_date_cadastre(year):
    """ Get cadastre's date.

    Args:
        year [Int]: Get date dataset from specify year.

    Returns:
        date [String]: Get information date.
    """
    response = requests.get(get_url(year), headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    res = soup.find_all('a', href=True)
    line_date = str(res[2].next_sibling)
    date = line_date.split()[0]
    print("get_date_cadastre: {}".format(date))
    return date


def check_cadastre_update(year):
    """ Check if a new cadastre has been released on datagouv.fr

    Args:
        year [Int]: Check new dataset from specify year.

    Returns:
        Bool [Boolean]: Return if there is a new dataset available or not.
    """
    file_path = os.path.join(currentDirectory, "{}.txt".format(year))
    try:
        text_file = open(file_path)
    except :
        text_file = open(file_path, "w")
        text_file.close()

    text_file = open(file_path, "r")
    content = text_file.read()
    date = get_date_cadastre(year)
    text_file.close()
    if date != content:
        print(date, content)
        text_file = open(file_path, "w")
        text_file.write(date)
        text_file.close()
        return True
    else:
        print("No need to update")
        return False


def main_process():
    """ If there is a new dataset, send it to RDS.
    """
    print("year: {}".format(year))
    print("DL file")
    filename = "75.csv.gz"
    r = requests.get(get_url(year) + filename, allow_redirects=True)

    local_csv = os.path.join(currentDirectory, filename)
    open(local_csv, 'wb').write(r.content)

    with gzip.open(local_csv, mode="rt", encoding='utf-8') as f:
        df = pd.read_csv(f)
        
        # Pre-Processing
        df = df[["date_mutation", "code_postal", "valeur_fonciere", "code_type_local", "surface_reelle_bati", "nombre_pieces_principales", "surface_terrain", "longitude", "latitude"]]
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


for year in range(2015, datetime.now().year): 
    print(year)
    if check_cadastre_update(year):
        main_process()
   