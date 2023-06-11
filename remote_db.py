from requester import Requester
from helpers import Json
from tqdm import tqdm

# RemoteDB class definition
class RemoteDB:
    def __init__(self):
        self.api_url = 'https://geneteka.genealodzy.pl/api/getAct.php'
        self.headers = {}
        self.params = {}
        self.set_headers('XMLHttpRequest', 'https://geneteka.genealodzy.pl/index.php?op=gt&lang=pol&bdm=B&w=05ma')
        self.strona_wyszukiwarki = Requester.get_soup('https://geneteka.genealodzy.pl/index.php?op=gt')
        self.tereny = self.pobierz_tereny()
        # TODO: Dodać do słownika kody parafii i terenu
        self.tereny_i_parafie = {teren: RemoteDB.pobierz_parafie(value) for teren, value in self.tereny.items()}
        self.licznik_terenow = 0
        self.licznik_parafii = 0
        self.max_tereny = 999
        self.max_parafie = 9999

    def set_params(self, start, length, w, rid):
        self.params['start'] = start
        self.params['length'] = length
        self.params['w'] = w
        self.params['rid'] = rid

    def set_headers(self, x_requested_with, referer):
        self.headers['x-requested-with'] = x_requested_with
        self.headers['referer'] = referer

    def pobierz_zakres_rekordow(self, parafia, teren, start , length):
        self.set_params(start, length, teren, parafia)
        response = Requester.fetch_data(self.api_url, self.params, headers=self.headers)
        for rekord in response['data']:
            rekord.append(parafia)
        return response
    
    def pobierz_tereny(self):
        options = self.strona_wyszukiwarki.select('select#sel_w option')
        return {option.text: option['value'] for option in options}
    
    def get_teren_id(self, nazwa_parafii):
        return self.tereny[nazwa_parafii] 

    def pobierz_parafie(teren):
        url = 'https://geneteka.genealodzy.pl/index.php?op=gt' + '&lang=pol&bdm=B&w=' + teren
        soup = Requester.get_soup(url)
        options = soup.select('select#sel_rid option')
        parafie = {}
        for option in options:
            nazwa_parafii = option.text
            atrybuty = {key: value for key, value in option.attrs.items() if key.startswith('data-')}
            atrybuty['value'] = option.attrs['value']
            parafie[nazwa_parafii] = atrybuty
        return parafie
    
    def pobierz_wszystkie_rekordy_z_parafii(self, parafia, teren):
        start = 0
        length = 50
        rekordy = []
        while True:
            response = self.pobierz_zakres_rekordow(parafia, teren, start, length)
            if response is None or len(response['data']) == 0:
                break
            rekordy.extend(response['data'])
            start += length
        return rekordy
    
    def get_number_of_records(self, parafia, teren):
        start = 0
        length = 50
        response = self.pobierz_zakres_rekordow(parafia, teren, start, length)
        if response is None or len(response['data']) == 0:
            return 0
        # Zwróć recordTotal jako int
        return int(response['recordsTotal'])
    
    def pobierz_wszystkie_rekordy_z_parafii_i_dodaj_do_bazy(self, parafia, teren, local_db, record_type):
        start = 0
        length = 50
        total_records = self.get_number_of_records(parafia, teren)
        progress_bar = tqdm(total=total_records, desc=f"Przetwarzanie {parafia}", unit="rekord")
        while True:
            response = self.pobierz_zakres_rekordow(parafia, teren, start, length)
            Json.write('response.json', response)
            if response is None or len(response['data']) == 0:
                break
            rekordy = response['data']
            for rekord in rekordy:
                rekord.append(teren)
            local_db.insert_into_db(rekordy, record_type)
            progress_bar.update(len(rekordy))
            start += length
        progress_bar.close()


remotedb = RemoteDB()
# Json.write('tereny.json', remotedb.tereny)
