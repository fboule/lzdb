import glob
import pandas as pd

class lzdict(dict):
    __loader = None

    class parquet(object):
        def get(self, name, folder = "data"):
            filelist = glob.glob("%s/*%s*" % (folder, name))
            if len(filelist) != 1:
                return None
            filepath = filelist[0]
            filename = filepath.split('_')[0].split('/')[1]
            print("Parquet::Get %s" % filename)
            return pd.read_parquet(filepath)

    class csv(object):
        def get(self, name, folder = "data"):
            filelist = glob.glob("%s/*%s*" % (folder, name))
            if len(filelist) != 1:
                return None
            filepath = filelist[0]
            filename = filepath.split('_')[0].split('/')[1]
            print("CSV::Get %s" % filename)
            return pd.read_csv(filepath)

    def __init__(self, loader = None):
        self.__loader = loader
        if loader is None:
            self.__loader = lzdict.parquet()

    def __getitem__(self, key):
        if not super().__contains__(key):
            self[key] = self.__loader.get(key)
        return super().__getitem__(key)

