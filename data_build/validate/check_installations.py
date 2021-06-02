from datetime import date
from typing import Any, Dict, cast

import pandas
from envinorma.data import Regime
from envinorma.data.installation import ActivityStatus, Installation, InstallationFamily, Seveso
from tqdm import tqdm


def _dataframe_record_to_installation(record: Dict[str, Any]) -> Installation:
    record['last_inspection'] = date.fromisoformat(record['last_inspection']) if record['last_inspection'] else None
    record['regime'] = Regime(record['regime'])
    record['seveso'] = Seveso(record['seveso'])
    record['family'] = InstallationFamily(record['family'])
    record['active'] = ActivityStatus(record['active'])
    return Installation(**record)


def check_installations_csv(filename: str) -> None:
    dataframe = pandas.read_csv(filename, dtype='str', na_values=None).fillna('')
    for record in tqdm(dataframe.to_dict(orient='records'), 'Checking installations csv'):
        _dataframe_record_to_installation(cast(Dict, record))
