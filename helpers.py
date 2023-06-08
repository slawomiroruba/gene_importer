class Json:
    @staticmethod
    def write(filename, data):
        with open(filename, 'w') as f:
            json.dump(data, f)
            f.write('\n')  # Dodanie nowej linii po zapisie danych

    @staticmethod
    def append(filename, data):
        with open(filename, 'a') as f:
            json.dump(data, f)
            f.write('\n')  # Dodanie nowej linii po zapisie danych
