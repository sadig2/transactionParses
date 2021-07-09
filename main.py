import pandas
from pandas.io.parsers import TextParser, read_csv
import json


class CSVParser:
    """
    A class responsible for reading and parsing csv files from the
    crypto wallets and exchanges.
    """

    def __init__(self, file_path):
        self.d = {}
        self.l = []

        df = pandas.read_csv(file_path)
        dd = df.to_dict('records')

        for i in dd:
            self.d['date'] = i['DATE']
            self.d['transaction_type'] = i['TYPE']
            if 'WITHDRAWAL' in i:
                self.d['received_amount'] = i['AMOUNT']
            else:
                self.d['received_amount'] = None
            self.d['received_currency_iso'] = i['CURRENCY']
            self.d['sent_amount'] = i['TYPE']
            self.d['sent_currency_iso'] = i['CURRENCY']
            self.l.append(self.d)

    def print_results(self):
        """
        Public method printing the output from parsing transactions
        """
        js = json.dumps(self.l)
        print(js)


if __name__ == '__main__':
    parser = CSVParser('exchange_files/exchange_1_transaction_file.csv')
    parser.print_results()
