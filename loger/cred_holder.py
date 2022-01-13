import configparser


class Cred:
    def __init__(self):
        cred = configparser.ConfigParser()
        cred.read('loger\\cred.ini')

        self.__url = cred.get('db', 'url')
        self.__user = cred.get('db', 'user')
        self.__password = cred.get('db', 'password')

        self.__query_card = cred.get('query', 'query_card')
        self.__query_transactions = cred.get('query', 'query_trans')
        self.__partitions = cred.get('path', 'partitions')
        self.__jar = cred.get('path', 'jar')
        self.log = cred.get('path', 'log')

    def get_creds(self):
        return {'url': self.__url,
                'user': self.__user,
                'password': self.__password,
                'query_cards': self.__query_card,
                'query_transactions': self.__query_transactions,
                'partitions': self.__partitions,
                'jar': self.__jar,
                }
