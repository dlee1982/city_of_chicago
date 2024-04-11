import pandas as pd
import numpy as np
import requests
from functools import cached_property

class DataLoader: 
    """
    Base data loader class not intended for direct use
    """

    def __init__(self, max_records) -> None: 
        self.url_path = ""

    @cached_property
    def dataset(self): 
        """
        loads the dataset only once 
        """

        return self.load_dataset()
    
    def load_dataset(self) -> None:
        """
        To be implemented by subclasses. Defines how datasets are loaded.
        """
        
        raise NotImplementedError('To be implemented by subclasses.')
    
class GetBinData(DataLoader): 
    "class for loading Chicago garbage black bin data"

    def __init__(self, max_records) -> None:
        super().__init__(max_records)
        self.url_path = "https://data.cityofchicago.org/resource/9ksk-na4q.json"
        self.limit = 1000
        self.max_records = max_records
        # self.sort_order = "&$order=date DESC"

    def load_dataset(self) -> pd.DataFrame:
        """
        limit: total limit of data
        max_records: total number of records
        base_url_add: api url address
        """
        
        base_url = self.url_path
        records = []
        offset = 0
        limit = self.limit
        max_records = self.max_records
        # sort_order = self.sort_order

        while offset < max_records:
            query_url = f"{base_url}?$limit={limit}&$offset={offset}"
            response = requests.get(query_url)

            if response.status_code == 200:
                data = response.json()
                if not data:  # Break the loop if no data is returned
                    break
                records.extend(data)
                offset += limit
            else:
                print(f"Failed to retrieve data at offset {offset}")
                break

        if records: 
            # return self.tweak_data(pd.DataFrame(records))
            return pd.DataFrame(records)
        else: 
            print("No records returned")