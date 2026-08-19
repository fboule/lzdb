import glob
import pandas as pd
from pathlib import Path

class lzdict(dict):
    class parquet(object):
        def get(self, obj):
            print("Parquet::Get %s" % obj['filename'])
            return pd.read_parquet(obj['filepath'])

    class csv(object):
        def get(self, obj):
            print("CSV::Get %s" % obj['filename'])
            return pd.read_csv(obj['filepath'])

    __loader = { 'parquet': parquet, 'csv': csv }

    def __init__(self, folder):
        filelist = glob.glob(f"{folder}/*")
        for filepath in filelist:
            filename = filepath.split('/')[1]
            filetitle = Path(filepath).stem
            filetype = Path(filename).suffix.strip('.')
            self.__setitem__(filetitle, { 'type': filetype, 'filepath': filepath, 'filename': filename, 'data': None })

    def __getitem__(self, key):
        realkey = next((k for k in self.keys() if key in k), None)
        if realkey is None:
            return None
        obj = super().__getitem__(realkey)
        if obj['data'] is None:
            obj['data'] = self.__loader[obj['type']]().get(obj)
        return obj['data']

