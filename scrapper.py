import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import json
import time
import pymysql
from local_db import LocalDB
from remote_db import RemoteDB

db = LocalDB()
remote_db = RemoteDB()

# 0. Zdefiniuj zmienne i funkcje pomocnicze
api_url = 'https://geneteka.genealodzy.pl/api/getAct.php'

def create_db_connection(host, user, password, db):
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db
    )
    return connection

# def insert_into_db(connection, rekordy):
#     with connection.cursor() as cursor:
#         for rekord in rekordy:
#             insert_query = "INSERT INTO rekordy (rok, akt, field_3, field_4, field_5, field_6, field_7, field_8, miejscowosc_parafia, uwagi, teren, parafia) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#             cursor.execute(insert_query, (rekord['rok'], rekord['akt'], rekord['field_3'], rekord['field_4'], rekord['field_5'], rekord['field_6'], rekord['field_7'], rekord['field_8'], rekord['miejscowosc_parafia'], rekord['uwagi'], rekord['teren'], rekord['parafia']))
#     connection.commit()




# def create_table(connection):
#     with connection.cursor() as cursor:
#         table_query = """
#         CREATE TABLE IF NOT EXISTS rekordy (
#             id INT AUTO_INCREMENT PRIMARY KEY,
#             rok VARCHAR(255),
#             akt VARCHAR(255),
#             field_3 VARCHAR(255),
#             field_4 VARCHAR(255),
#             field_5 VARCHAR(255),
#             field_6 VARCHAR(255),
#             field_7 VARCHAR(255),
#             field_8 VARCHAR(255),
#             miejscowosc_parafia VARCHAR(255),
#             uwagi VARCHAR(2000),
#             teren VARCHAR(255),
#             parafia VARCHAR(255)
#         )
#         """
#         cursor.execute(table_query)
#     connection.commit()

def get_soup(url):
    return soup_parse(http_request(url))

def http_request(url, params=None):
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response

def soup_parse(response):
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def fetch_data(url, params, headers, max_retries=20):
    for attempt in range(max_retries):
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            return response.json()  # returns the response as a Python dictionary
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}. Attempt: {attempt + 1}")
            time.sleep(1)  # sleep for 1 second before next attempt
    print(f"Failed to fetch data after {max_retries} attempts.")
    return None

def save_data_to_json(filename, data):
    with open(filename, 'a') as f:
        json.dump(data, f)
        f.write('\n')  # Dodanie nowej linii po zapisie danych


def pobierz_tereny(soup):
    options = soup.select('select#sel_w option')
    return {option.text: option['value'] for option in options}

def pobierz_parafie(teren):
    url = 'https://geneteka.genealodzy.pl/index.php?op=gt' + '&lang=pol&bdm=B&w=' + teren
    soup = get_soup(url)
    options = soup.select('select#sel_rid option')
    parafie = {}
    for option in options:
        nazwa_parafii = option.text
        atrybuty = {key: value for key, value in option.attrs.items() if key.startswith('data-')}
        atrybuty['value'] = option.attrs['value']
        parafie[nazwa_parafii] = atrybuty
    return parafie




def pobierz_zakres_rekordow(parafia, teren, start , length, bdm='S'):
    params = {
        'start': start,
        'length': length,
        'w': teren,
        'rid': parafia,
    }

    headers = {
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://geneteka.genealodzy.pl/index.php?op=gt&lang=pol&bdm=B&w=01ds&rid=11111&search_lastname=&search_name=&search_lastname2=&search_name2=&from_date=&to_date=&ordertable=[[0,%22asc%22],[1,%22asc%22],[2,%22asc%22]]&searchtable=&rpp1=50&rpp2=50",
    }

    response = fetch_data(api_url, params, headers=headers)

    return response


def pobierz_wszystkie_rekordy_z_parafii(parafia, teren):
    start = 0
    length = 50
    rekordy = []
    while True:
        response = pobierz_zakres_rekordow(parafia, teren, start, length)
        if response is None or len(response['data']) == 0:
            break
        rekordy.extend(response['data'])
        start += length
    return rekordy

def get_total_records_from_parafia_by_type():
    start = 0
    length = 50
    response = pobierz_zakres_rekordow(parafia, teren, start, length)
    if response is None or len(response['data']) == 0:
        return 0
    # Zwróć recordTotal jako int
    return int(response['recordsTotal'])



# save_data_to_json('dupa.json', pobierz_parafie('01ds'))

# 1. Pobierz wszystkie tereny
strona_wyszukiwarki = get_soup('https://geneteka.genealodzy.pl/index.php?op=gt')
tereny = pobierz_tereny(strona_wyszukiwarki)

# 2. Dla każdego terenu pobierz wszystkie parafie
tereny_i_parafie = {teren: pobierz_parafie(value) for teren, value in tereny.items()}

# 3. Dla każdej parafii pobierz wszystkie rekordy
wszystkie_rekordy = {}
licznik_terenow = 0
licznik_parafii = 0
max_tereny = 999  # maksymalna liczba terenów do przetworzenia
max_parafie = 999  # maksymalna liczba parafii do przetworzenia

connection = create_db_connection('localhost', 'gene_main', 'JakubKnoll1!', 'gene_main')  # Podaj swoje dane do połączenia


for teren, parafie in tereny_i_parafie.items():
    print(f'Przetwarzam teren: {teren}')
    if licznik_terenow >= max_tereny:
        break 
    if licznik_terenow == 0:
        # przejdź do następnej iteracji
        licznik_terenow += 1
        continue 

    for parafia, attrs in parafie.items():
        print(f'Przetwarzam parafię: {parafia}')
        if licznik_parafii >= max_parafie:
            break  

        for attr_key, attr_value in attrs.items():
            if attr_value != '':
                record_type = ''
                if attr_key == 'data-b':
                    record_type = 'birth'
                elif attr_key == 'data-s':
                    record_type = 'marriage'
                elif attr_key == 'data-d':
                    record_type = 'death'
                if record_type:
                    rekordy = pobierz_wszystkie_rekordy_z_parafii(attr_value, teren)
                    if rekordy:
                        for rekord in rekordy:
                            rekord.append(teren)
                        insert_into_db(connection, rekordy, record_type)

        licznik_parafii += 1  
    licznik_terenow += 1 

connection.close()

def sync_databases():
