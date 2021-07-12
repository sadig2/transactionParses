from collections import defaultdict

import pandas
from pandas.io.parsers import TextParser, read_csv
import json


class CSVParser:
    """
    A class responsible for reading and parsing csv files from the
    crypto wallets and exchanges.
    """

    def __init__(self, file_path):
        self.results = []
        self.error = None
        column_names, rows = self._load_csv(file_path)
        scheme, parser_fn = CSVParser._detect_scheme(column_names)
        if scheme is None:
            self.error = "couldn't detect scheme"
        self.results = parser_fn(rows)

    @staticmethod
    def _load_csv(file_path):
        # TODO return generator
        df = pandas.read_csv(file_path)
        column_names = list(df.columns)
        rows = df.to_dict('records')
        return column_names, rows

    @staticmethod
    def _detect_scheme(column_names):
        """
        detects scheme by column names and their order
        returns type of scheme (e.g "type1") or None if scheme is not supported
        """
        supported_schemes = {
            "type1": {
                "columns": ["TRANSACTION_ID", "TYPE", "DATE", "AMOUNT", "CURRENCY"],
                "parser": CSVParser._parse_csv_type1,
            },
            "type2": {
                "columns": ["TYPE", "TIME", "SOLD AMOUNt", "BOUGHT AMOUNT", "CURRENCIES"],
                "parser": CSVParser._parse_csv_type2,
            }
        }
        for k, v in supported_schemes.items():
            if column_names == v["columns"]:
                return k, v["parser"]

    @staticmethod
    def _parse_csv_type1(rows):
        # ["TRANSACTION_ID", "TYPE", "DATE", "AMOUNT", "CURRENCY"]
        result = []

        type_csv_to_json = {
            "DEPOSIT": "Deposit",
            "TRADE": "Trade",
            "WITHDRAWAL": "Withdrawal",
        }

        # transactions = defaultdict(list)
        # for row in rows:
        #     tran_id = row['TRANSACTION_ID']
        #     transactions[tran_id].append(row)

        transactions = {}
        for row in rows:
            tran_id = row['TRANSACTION_ID']
            if tran_id not in transactions:
                transactions[tran_id] = []
            transactions[tran_id].append(row)

        for rows in transactions.values():
            first_row = rows[0]
            csv_type = first_row["TYPE"]
            tran_type = type_csv_to_json[csv_type]

            received_amount = None
            received_currency_iso = None
            sent_amount = None
            sent_currency_iso = None
            for row in rows:
                amount = row["AMOUNT"]
                curr = row["CURRENCY"]
                if amount < 0:
                    sent_amount = -amount
                    sent_currency_iso = curr
                else:
                    received_amount = amount
                    received_currency_iso = curr

            json_row = {
                'date': first_row["DATE"],
                'transaction_type': tran_type,
                'received_amount': received_amount,
                'received_currency_iso': received_currency_iso,
                'sent_amount': sent_amount,
                'sent_currency_iso': sent_currency_iso,
            }
            result.append(json_row)
        return result

    @staticmethod
    def _parse_csv_type2(rows):
        # ["TYPE", "TIME", "SOLD AMOUNt", "BOUGHT AMOUNT", "CURRENCIES"]
        result = []

        type_csv_to_json = {
            "DEPOSIT": "Buy",
            "TRADE": "Sell",
            "WITHDRAWAL": "Withdrawal",
        }

        received_currency_iso = None
        received_currency = None
        sent_currency_iso = None
        sent_currency = None

        for row in rows:
            if not pandas.isna(row['SOLD AMOUNt']):
                sent_currency = row['SOLD AMOUNt']
                sent_currency_iso = row['CURRENCIES']
            if not pandas.isna(row['BOUGHT AMOUNT']):
                received_currency = row['BOUGHT AMOUNT']
                received_currency_iso = row['CURRENCIES']

            cur_arr = row['CURRENCIES'].split('-')
            if len(cur_arr) > 2:
                sent_currency_iso = cur_arr[0]
                received_currency_iso = cur_arr[2]

            json_row = {
                'date': row["TIME"],
                'transaction_type': row['TYPE'],
                'received_amount': received_currency,
                'received_currency_iso': received_currency_iso,
                'sent_amount': sent_currency,
                'sent_currency_iso': sent_currency_iso,
            }
            result.append(json_row)

        return result

    def print_results(self):
        """
        Public method printing the output from parsing transactions
        """
        if self.error is not None:
            print(self.error)
        js = json.dumps(self.results, indent='  ')
        print(js)


if __name__ == '__main__':
    parser = CSVParser('exchange_files/exchange_2_transaction_file.csv')
    parser.print_results()
