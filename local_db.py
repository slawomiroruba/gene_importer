import pymysql

class LocalDB:
    def __init__(self):
        self.connection = self.create_db_connection('localhost', 'gene_main', 'JakubKnoll1!', 'gene_main')

    def create_db_connection(self, host, user, password, db):
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db
        )
        return connection

    def insert_into_db(self, rekordy, record_type):
        with self.connection.cursor() as cursor:
            if record_type == 'birth':
                column_names = ['rok', 'akt', 'imie', 'nazwisko', 'imie_ojca', 'imie_matki', 'nazwisko_matki', 'parafia', 'miejscowosc', 'uwagi', 'teren']
                varchar_types = ['TEXT(20000)'] * 11
                table_name = 'birth_records'
            elif record_type == 'marriage':
                column_names = ['rok', 'akt', 'imie_pana', 'nazwisko_pana', 'rodzice_pana', 'imie_pani', 'nazwisko_pani', 'rodzice_pani', 'parafia', 'uwagi', 'teren']
                varchar_types = ['TEXT(20000)'] * 11
                table_name = 'marriage_records'
            elif record_type == 'death':
                column_names = ['rok', 'akt', 'imie', 'nazwisko', 'imie_ojca', 'imie_matki', 'nazwisko_matki', 'parafia', 'miejscowosc', 'uwagi', 'teren']
                varchar_types = ['TEXT(20000)'] * 11
                table_name = 'death_records'

            placeholders = ', '.join(['%s'] * len(column_names))
            column_names_str = ', '.join(column_names)
            varchar_types_str = ', '.join(varchar_types)

            insert_query = f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})"
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, {', '.join([f'{col} TEXT(3000)' for col in column_names])})")

            for rekord in rekordy:
                cursor.execute(insert_query, tuple(map(str, rekord)))

        self.connection.commit()
    
    def get_all_records(self, record_type):
        with self.connection.cursor() as cursor:
            if record_type == 'birth':
                table_name = 'birth_records'
            elif record_type == 'marriage':
                table_name = 'marriage_records'
            elif record_type == 'death':
                table_name = 'death_records'
            cursor.execute(f"SELECT * FROM {table_name}")
            return cursor.fetchall()

    def close_connection(self):
        self.connection.close()
