import glob
import pandas as pd
from pathlib import Path

class lzdictitem(object):
    __type = None
    __name = None
    __path = None
    __title = None
    data = None

    def __init__(self, filepath):
        self.__path = filepath
        self.__name = filepath.split('/')[1]
        self.__title = Path(filepath).stem
        self.__type = Path(self.__name).suffix.strip('.')

    @property
    def info(self):
        return {
            'type': self.__type,
            'name': self.__name,
            'path': self.__path,          
            'title': self.__title        
        }

class lzdict(dict):
    class parquet(object):
        def get(self, info):
            print("Parquet::Get %s" % info['name'])
            return pd.read_parquet(info['path'])

    class csv(object):
        def get(self, info):
            print("CSV::Get %s" % info['name'])
            return pd.read_csv(info['path'])

    __loader = { 
        'parquet': parquet, 
        'csv': csv 
    }

    def __init__(self, folder = 'data'):
        filelist = glob.glob(f"{folder}/*")
        for filepath in filelist:
            item = lzdictitem(filepath)
            self.__setitem__(item.info['title'], item)

    def resolve(self, key):
        return next((k for k in self.keys() if key in k), None)

    def fetch(self, key):
        realkey = self.resolve(key)
        return super().__getitem__(realkey) if realkey else None

    def __getitem__(self, key):
        item = self.load(key)
        return item.data if item else None

    @property
    def info(self):
        return {
            k: v.info for k, v in self.items()
        }

    def clear(self, key):
        item = self.fetch(key)
        if item is not None:
            item.data = None

    def load(self, key):
        item = self.fetch(key)
        if item and item.data is None:
            item.data = self.__loader[item.info['type']]().get(item.info)
        return item
