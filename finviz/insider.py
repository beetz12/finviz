from finviz.helper_functions.request_functions import Connector, http_request_get
from finviz.helper_functions.error_handling import NoResults, InvalidTableType
from finviz.helper_functions.save_data import export_to_db, export_to_csv
from finviz.helper_functions.display_functions import create_table_string
from urllib.parse import urlencode, urlparse, parse_qs as urlparse_qs
import finviz.helper_functions.scraper_functions as scrape
from lxml import html

TRANSACTION_TYPES = {
    'buy': '1',
    'sell': '2'
}

class Insider(object):
    """ Used to download data from http://www.finviz.com/screener.ashx. """

    # @classmethod


    def __init__(self, tickers=None, transaction_type=None, rows=None, order=''):
        """
        Initilizes all variables to its values

        :param tickers: collection of ticker strings eg.: ['AAPL', 'AMD', 'WMT']
        :type tickers: list
        :param transaction_type: transaction_type e.g. buy, sell, or None = all
        :type transaction_type: list
        :param rows: total number of rows to get
        :type rows: int
        :param order: table order eg.: '-price' (to sort table by descending price)
        :type order: str
        """

        # print('in init')
        # print('transaction_type = ')
        # print(transaction_type)
        # print(order)

        if tickers is None:
            self._tickers = []
        else:
            self._tickers = tickers

        if transaction_type is None:
            self._transaction_type = []
        else:
            self._transaction_type = transaction_type

        self._rows = rows
        self._order = order

        self.data = self.__search_insider()

    def __call__(self, tickers=None, rows=None, order=''):
        """
        Adds more filters to the screener. Example usage:

        stock_list = Screener(filters=['cap_large'])  # All the stocks with large market cap
        # After analyzing you decide you want to see which of the stocks have high dividend yield
        # and show their performance:
        stock_list(filters=['fa_div_high'], table='Performance')
        # Shows performance of stocks with large market cap and high dividend yield
        """

        if tickers:
            [self._tickers.append(item) for item in tickers]

        if order:
            self._order = order

        if rows:
            self._rows = rows

        # self.analysis = []
        self.data = self.__search_insider()

    add = __call__

    def __str__(self):
        """ Returns a readable representation of a table. """

        table_list = [self.headers]

        for row in self.data:
            table_list.append([row[col] or '' for col in self.headers])

        return create_table_string(table_list)

    def __repr__(self):
        """ Returns a string representation of the parameter's values. """

        values = f'tickers: {tuple(self._tickers)}\n' \
                 f'filters: {tuple(self._filters)}\n' \
                 f'rows: {self._rows}\n' \
                 f'order: {self._order}\n' \
                 f'signal: {self._signal}\n' \
                 f'table: {self._table}\n' \
                 f'table: {self._custom}'

        return values

    def __len__(self):
        """ Returns an int with the number of total rows. """

        return int(self._rows)

    def __getitem__(self, position):
        """ Returns a dictionary containting specific row data. """

        return self.data[position]

    get = __getitem__

    def to_sqlite(self, filename):
        """ Exports the generated table into a SQLite database.

        :param filename: SQLite database file path
        :type filename: str
        """

        export_to_db(self.headers, self.data, filename)

    def to_csv(self, filename=None):
        """ Exports the generated table into a CSV file.
        Returns a CSV string if filename is None.

        :param filename: CSV file path
        :type filename: str
        """

        if filename.endswith('.csv'):
            filename = filename[:-4]

        # if len(self.analysis) > 0:
        #     export_to_csv(['ticker', 'date', 'category', 'analyst', 'rating', 'price_from', 'price_to'],
        #                   self.analysis, '%s-analysts.csv' % filename)

        return export_to_csv(self.headers, self.data, '%s.csv' % filename)

    def get_charts(self, period='d', size='l', chart_type='c', ta='1'):
        """
        Downloads the charts of all tickers shown by the table.

        :param period: table period eg. : 'd', 'w' or 'm' for daily, weekly and monthly periods
        :type period: str
        :param size: table size eg.: 'l' for large or 's' for small - choose large for better quality but higher size
        :type size: str
        :param chart_type: chart type: 'c' for candles or 'l' for lines
        :type chart_type: str
        :param ta: technical analysis eg.: '1' to show ta '0' to hide ta
        :type ta: str
        """

        payload = {
            'ty': chart_type,
            'ta': ta,
            'p': period,
            's': size
        }

        base_url = 'https://finviz.com/chart.ashx?' + urlencode(payload)
        chart_urls = []

        for row in self.data:
            chart_urls.append(base_url + f"&t={row.get('Ticker')}")

        async_connector = Connector(scrape.download_chart_image, chart_urls)
        async_connector.run_connector()


    def get_ticker_details(self):
        """
        Downloads the details of all tickers shown by the table.
        """

        base_url = 'https://finviz.com/quote.ashx?'
        ticker_urls = []

        for row in self.data:
            ticker_urls.append(base_url + f"&t={row.get('Ticker')}")

        async_connector = Connector(scrape.download_ticker_details, ticker_urls, cssselect=True)
        ticker_data = async_connector.run_connector()

        for entry in ticker_data:
            for key, value in entry.items():
                for ticker_generic in self.data:
                    if ticker_generic.get('Ticker') == key:
                        if 'Sales' not in self.headers:
                            self.headers.extend(list(value[0].keys()))

                        ticker_generic.update(value[0])
                        self.analysis.extend(value[1])

        return self.data

    def __check_rows(self):
        """
        Checks if the user input for row number is correct.
        Otherwise, modifies the number or raises NoResults error.
        """

        #TODO: figure out how to parse this...
        self._total_rows = scrape.get_total_insider_rows(self._page_content)

        if self._total_rows == 0:
            raise NoResults(self._url.split('?')[1])
        elif self._rows is None or self._rows > self._total_rows:
            return self._total_rows
        else:
            return self._rows

    def __check_table(self, input_table):
        """ Checks if the user input for table type is correct. Otherwise, raises an InvalidTableType error. """

        try:
            table = TABLE_TYPES[input_table]
            return table
        except KeyError:
            raise InvalidTableType(input_table)

    def __get_table_headers(self):
        """ Private function used to return table headers. """
        print(self._page_content.cssselect('.body-table tr')[0])
        return self._page_content.cssselect('.body-table tr')[0].xpath('td//text()')

    def __get_transaction_type(self):
        if self._transaction_type in TRANSACTION_TYPES:
            return TRANSACTION_TYPES[self._transaction_type]
        else:
            return ''

    def __search_insider(self):
        """ Private function used to return data from the FinViz screener. """

        self._page_content, self._url = http_request_get('https://finviz.com/insidertrading.ashx', payload={
                                                   'tc': self.__get_transaction_type(),
                                                   })

        self._rows = self.__check_rows()
        # print(str(self._page_content))
        self.headers = self.__get_table_headers()
        # print(self.headers)
        # page_urls = scrape.get_page_urls(self._page_content, self._rows, self._url)
        # print(page_urls)

        data = scrape.get_insider(self._page_content, self.headers)
        # print(foo)

        # async_connector = Connector(scrape.get_table,
        #                             page_urls,
        #                             self.headers,
        #                             self._rows)
        # pages_data = async_connector.run_connector()

        # data = []
        # for page in pages_data:
        #     for row in page:
        #         data.append(row)
        print(data)
        return data
