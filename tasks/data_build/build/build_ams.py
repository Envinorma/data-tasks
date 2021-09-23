import os
from typing import List

from envinorma.models import ArreteMinisteriel
from envinorma.utils import typed_tqdm, write_json

from tasks.data_build.config import DATA_FETCHER
from tasks.data_build.filenames import ENRICHED_OUTPUT_FOLDER


def _remove_previously_enriched_ams() -> None:
    for file_ in typed_tqdm(os.listdir(ENRICHED_OUTPUT_FOLDER), 'Removing previously enriched files'):
        os.remove(os.path.join(ENRICHED_OUTPUT_FOLDER, file_))


def _create_if_inexistent(folder: str):
    if not os.path.exists(folder):
        os.mkdir(folder)


def _write_ams(ams: List[ArreteMinisteriel]) -> None:
    for am in typed_tqdm(ams, 'Writing AMs'):
        full_path = os.path.join(ENRICHED_OUTPUT_FOLDER, am.id or '') + '.json'
        write_json(am.to_dict(), full_path, indent=2)


def generate_ams() -> None:
    _create_if_inexistent(ENRICHED_OUTPUT_FOLDER)
    ams = DATA_FETCHER.build_enriched_ams()
    _remove_previously_enriched_ams()
    _write_ams(ams)
