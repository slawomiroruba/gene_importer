from local_db import LocalDB
from remote_db import RemoteDB
from requester import Requester
from helpers import Json

local_db = LocalDB()
remote_db = RemoteDB()
requester = Requester()
json_tools = Json()

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
                        local_db.insert_into_db(rekordy, record_type)

        remote_db.licznik_parafii += 1  
    remote_db.licznik_terenow += 1 

local_db.close_connection()

