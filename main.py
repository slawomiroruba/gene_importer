from local_db import LocalDB
from remote_db import RemoteDB
from requester import Requester
from helpers import Json
from datetime import datetime


# Zapisz w pliku cron.log datę i godzinę uruchomienia skryptu

# Pobierz aktualną datę i godzinę
current_time = datetime.now()

# Formatuj datę i godzinę do postaci ciągu znaków
time_string = current_time.strftime("%Y-%m-%d %H:%M:%S")

# Otwórz plik cron.log do dopisywania (tryb 'a')
with open("cron.log", "a") as file:
    # Zapisz sformatowany ciąg znaków do pliku, dodaj znak nowej linii na końcu
    file.write("Rozpoczęto wykonywanie skryptu: " + time_string + "\n")


local_db = LocalDB()
remote_db = RemoteDB()
requester = Requester()
json_tools = Json()



json_tools.write('tereny_i_parafie.json', remote_db.tereny_i_parafie)
for teren, parafie in remote_db.tereny_i_parafie.items():
    print(f'Przetwarzam teren: {teren}')
    if remote_db.licznik_terenow >= remote_db.max_tereny:
        break 

    for parafia, attrs in parafie.items():
            
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
                    count_local_records = local_db.count_records_from_parafia(parafia, record_type)
                    count_remote_records = remote_db.get_number_of_records(attr_value, remote_db.get_teren_id(teren))
                    if(count_local_records != count_remote_records):
                        print(f'Przetwarzam parafię: {parafia} ({record_type})')
                        local_db.delete_all_records_from_parafia(parafia, record_type)
                        remote_db.pobierz_wszystkie_rekordy_z_parafii_i_dodaj_do_bazy(attr_value, teren, local_db, record_type)
                    else:
                        print(f'Parafia: {parafia} ({record_type}) jest aktualna')
        remote_db.licznik_parafii += 1  
    remote_db.licznik_terenow += 1 

local_db.close_connection()

# Zapisz w pliku cron.log datę i godzinę zakończenia skryptu

# Pobierz aktualną datę i godzinę
current_time_end = datetime.now()

# Formatuj datę i godzinę do postaci ciągu znaków
time_string_end = current_time_end.strftime("%Y-%m-%d %H:%M:%S")

# Otwórz plik cron.log do dopisywania (tryb 'a')
with open("cron.log", "a") as file:
    # Zapisz sformatowany ciąg znaków do pliku, dodaj znak nowej linii na końcu
    file.write("Zakończono wykonywanie skryptu: " + time_string_end + "\n")