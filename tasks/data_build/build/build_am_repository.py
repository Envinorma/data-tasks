'''
Generate AM open data repository
cf https://github.com/Envinorma/arretes-ministeriels
'''
import json
import os
from typing import Any, Dict, List, Union

from envinorma.models.arrete_ministeriel import ArreteMinisteriel
from envinorma.utils import typed_tqdm

from tasks.data_build.config import AM_REPOSITORY_FOLDER, DATA_FETCHER

_METADATA_FOLDER = os.path.join(AM_REPOSITORY_FOLDER, 'metadata')
_AMS_FOLDER = os.path.join(AM_REPOSITORY_FOLDER, 'ams')


def _create_if_inexistent(folder: str) -> None:
    if not os.path.exists(folder):
        os.mkdir(folder)


def _dump(object_: Union[Dict, List], filename: str) -> None:
    with open(filename, 'w') as file_:
        json.dump(object_, file_, ensure_ascii=True, indent=2, sort_keys=True)


def _dump_am_metadata(am_id: str, am: Dict[str, Any]) -> None:
    filename = os.path.join(_METADATA_FOLDER, am_id + '.json')
    _dump(am, filename)


def _generate_metadata_folder() -> None:
    _create_if_inexistent(_METADATA_FOLDER)
    metadata = DATA_FETCHER.load_all_am_metadata()
    for am_id, md in typed_tqdm(metadata.items(), 'Dumping AM metadata'):
        _dump_am_metadata(am_id, md.to_dict())


def _dump_am(am: Dict[str, Any]) -> None:
    filename = os.path.join(_AMS_FOLDER, am['id'] + '.json')
    _dump(am, filename)


def _load_ams() -> List[ArreteMinisteriel]:
    return DATA_FETCHER.build_enriched_ams()


def _generate_ams_folder() -> None:
    _create_if_inexistent(_AMS_FOLDER)
    ams = _load_ams()
    for am in typed_tqdm(ams, 'Dumping AMs'):
        _dump_am(am.to_dict())


def _empty_directory(folder: str) -> None:
    if os.path.exists(folder):
        for file_ in os.listdir(folder):
            os.remove(os.path.join(folder, file_))


def generate_am_repository() -> None:
    _empty_directory(_METADATA_FOLDER)
    _empty_directory(_AMS_FOLDER)
    _generate_metadata_folder()
    _generate_ams_folder()
