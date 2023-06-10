import pymysql

class LocalDB:
    def __init__(self):
        self.connection = self.create_db_connection('localhost', 'gene_main', 'JakubKnoll1!', 'gene_main')
        self.create_db_tables()

    def create_db_connection(self, host, user, password, db):
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db
        )
        return connection

    def create_db_tables(self):
        record_types = ['birth', 'marriage', 'death']
        
        with self.connection.cursor() as cursor:
            for record_type in record_types:
                if record_type == 'birth':
                    column_names = ['rok', 'akt', 'imie', 'nazwisko', 'imie_ojca', 'imie_matki', 'nazwisko_matki', 'parafia', 'miejscowosc', 'uwagi', 'rid', 'teren']
                    table_name = 'birth_records'
                elif record_type == 'marriage':
                    column_names = ['rok', 'akt', 'imie_pana', 'nazwisko_pana', 'rodzice_pana', 'imie_pani', 'nazwisko_pani', 'rodzice_pani', 'parafia', 'uwagi', 'rid', 'teren']
                    table_name = 'marriage_records'
                elif record_type == 'death':
                    column_names = ['rok', 'akt', 'imie', 'nazwisko', 'imie_ojca', 'imie_matki', 'nazwisko_matki', 'parafia', 'miejscowosc', 'uwagi', 'rid', 'teren']
                    table_name = 'death_records'

                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, {', '.join([f'{col} TEXT(3000)' for col in column_names])})")

            self.connection.commit()
        
    def get_table_name(self, record_type):
        if record_type == 'birth':
            return 'birth_records'
        elif record_type == 'marriage':
            return 'marriage_records'
        elif record_type == 'death':
            return 'death_records'

    def insert_into_db(self, rekordy, record_type):
        with self.connection.cursor() as cursor:
            if record_type == 'birth':
                column_names = ['rok', 'akt', 'imie', 'nazwisko', 'imie_ojca', 'imie_matki', 'nazwisko_matki', 'parafia', 'miejscowosc', 'uwagi', 'rid', 'teren']
                # varchar_types = ['TEXT(20000)'] * 13
                table_name = 'birth_records'
            elif record_type == 'marriage':
                column_names = ['rok', 'akt', 'imie_pana', 'nazwisko_pana', 'rodzice_pana', 'imie_pani', 'nazwisko_pani', 'rodzice_pani', 'parafia', 'uwagi', 'rid', 'teren']
                # varchar_types = ['TEXT(20000)'] * 13
                table_name = 'marriage_records'
            elif record_type == 'death':
                column_names = ['rok', 'akt', 'imie', 'nazwisko', 'imie_ojca', 'imie_matki', 'nazwisko_matki', 'parafia', 'miejscowosc', 'uwagi', 'rid', 'teren']
                # varchar_types = ['TEXT(20000)'] * 13
                table_name = 'death_records'

            placeholders = ', '.join(['%s'] * len(column_names))
            column_names_str = ', '.join(column_names)
            # varchar_types_str = ', '.join(varchar_types)

            insert_query = f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})"
            
            for rekord in rekordy:
                # print(len(rekord), len(column_names))
                cursor.execute(insert_query, tuple(map(str, rekord)))

        self.connection.commit()
    
    def get_all_records(self, record_type):
        with self.connection.cursor() as cursor:
            table_name = self.get_table_name(record_type)
            cursor.execute(f"SELECT * FROM {table_name}")
            return cursor.fetchall()

    def count_records_from_parafia(self, parafia_name, record_type):
        with self.connection.cursor() as cursor:
            table_name = self.get_table_name(record_type)
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE parafia = '{parafia_name}'")
            return cursor.fetchone()[0]

    def delete_all_records_from_parafia(self, parafia_name, record_type):
        with self.connection.cursor() as cursor:
            table_name = self.get_table_name(record_type)
            cursor.execute(f"DELETE FROM {table_name} WHERE parafia = '{parafia_name}'")
            self.connection.commit()

    def close_connection(self):
        self.connection.close()
