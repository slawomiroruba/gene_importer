# RemoteDB class definition
class RemoteDB:
    __init__(self):
        self.api_url = 'https://geneteka.genealodzy.pl/api/getAct.php'
        self.set_headers('XMLHttpRequest', 'https://geneteka.genealodzy.pl/index.php?op=gt&lang=pol&bdm=B&w=05ma')
    
    def set_params(self, start, length, w, rid):
        self.params['start'] = start
        self.params['length'] = length
        self.params['w'] = w
        self.params['rid'] = rid

    def set_headers(self, x_requested_with, referer):
        self.headers['x-requested-with'] = x_requested_with
        self.headers['referer'] = referer

    def pobierz_zakres_rekordow(self, parafia, teren, start , length, bdm='S'):
        self.set_params(start, length, teren, parafia)

        response = fetch_data(api_url, params, headers=self.headers)

        return response