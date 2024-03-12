import logging
import pandas as pd
from abc import ABC, abstractmethod
import requests
import io
import locale

class UnstructuredExcelFile(ABC):
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.data = None

    @abstractmethod
    def parse(self, data):
        pass
    
    def load_from_url(self):
        file_contents = requests.get(self.config['url']).content
        self.data = self.parse(pd.ExcelFile(io.BytesIO(file_contents)))


class KBAFZ28File(UnstructuredExcelFile):

    @staticmethod
    def _parse_dates(dates):
        lambda month: pd.to_datetime(month, format='%Y%m', errors='coerce')
        year = 0
        parsed_dates = []
        for index, d in dates.items():
            d = str(d)
            if d.startswith('Jahr '):
                year = [int(s) for s in d.split() if s.isdigit()][0]
                parsed_dates.append(pd.NaT)
            else:
                date = pd.to_datetime(f"{d} {year}", format='%B %Y', errors='ignore')
                if type(date) is pd.Timestamp:
                    parsed_dates.append(date)
                else:
                    parsed_dates.append(pd.NaT)

        return parsed_dates            


    def parse(self, data):
        locale.setlocale(locale.LC_ALL, 'de_DE')
        parsed_data = data.parse(
            sheet_name=self.config['sheet_name'],
            skiprows=11,
            header=0,
            names=[
                'monat_raw',
                'total',
                'total_alt','share_alt',
                'total_ev','share_ev',
                'total_bev','total_fecv','total_phev'
            ],
            usecols='B:J',
            na_values=["-"]
        )
        f = lambda x: pd.to_datetime(x, format='%Y%m', errors='coerce')
        parsed_data['date'] = KBAFZ28File._parse_dates(parsed_data['monat_raw'])
        parsed_data.drop(columns=['monat_raw'], inplace=True)
        return parsed_data[parsed_data['date'].notna()].set_index('date')
    

class ImportUtils(object):
    
    @staticmethod
    def load_kba_fz28_file(config):
        file = KBAFZ28File(config)
        file.load_from_url()

        return file