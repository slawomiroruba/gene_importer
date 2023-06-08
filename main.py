from local_db import LocalDB
from remote_db import RemoteDB
from requester import Requester
from helpers import Json

local_db = LocalDB()
remote_db = RemoteDB()
requester = Requester()
json_tools = Json()

class Importer:
    def porownaj_ilosc_rekordow_lokalnie_do_zdalnie_dla_parafii(self, teren, rid):
        # pobierz ilosc rekordow z lokalnej bazy dla danej parafii
        # pobierz ilosc rekordow z zewnetrznej bazy dla danej parafii
        # porownaj ilosci rekordow
        # jesli ilosci sa rozne to zwroc False
        # jesli ilosci sa takie same to zwroc True
        return

    def porownaj_rekord_parafii(self, rekord):
        # wyszukaj rekord w lokalnej bazie
        # jesli rekord istnieje to zwroc True
        # jesli rekord nie istnieje to zwroc False
        return

    def przepisz_rekordy_parafii(self, nazwa_tabeli, ilosc_rekordow):
        # usuń wszystkie rekordy z lokalnej bazy dla danej parafii
        # pobierz wszystkie rekordy z zewnetrznej bazy dla danej parafii
        # zapisz wszystkie rekordy do lokalnej bazy
        return


json_tools.write('tereny_i_parafie.json', remote_db.tereny_i_parafie)
for teren, parafie in remote_db.tereny_i_parafie.items():
    print(f'Przetwarzam teren: {teren}')
    if remote_db.licznik_terenow >= remote_db.max_tereny:
        break 
    if remote_db.licznik_terenow == 0:
        # przejdź do następnej iteracji
        remote_db.licznik_terenow += 1
        continue 

    for parafia, attrs in parafie.items():
        print(f'Przetwarzam parafię: {parafia}')
        if remote_db.licznik_parafii >= remote_db.max_parafie:
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
                    rekordy = remote_db.pobierz_wszystkie_rekordy_z_parafii(attr_value, teren)
                    if rekordy:
                        for rekord in rekordy:
                            rekord.append(teren)
                            rekord.append(parafia)
                        local_db.insert_into_db(rekordy, record_type)

        remote_db.licznik_parafii += 1  
    remote_db.licznik_terenow += 1 

local_db.close_connection()

