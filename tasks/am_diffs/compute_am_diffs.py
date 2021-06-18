from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Iterable, List, Optional, TypeVar

import requests
from envinorma.models import AMMetadata
from leginorma import LegifranceText
from leginorma.api import LegifranceRequestError
from tabulate import tabulate
from text_diff import TextDifferences, text_differences
from tqdm import tqdm

from tasks.common.config import DATA_FETCHER

from .config import AM_SLACK_URL, get_legifrance_client

T = TypeVar('T')


def typed_tqdm(
    collection: Iterable[T], desc: Optional[str] = None, leave: bool = True, disable: bool = False
) -> Iterable[T]:
    return tqdm(collection, desc=desc, leave=leave, disable=disable)


_BO_URL = 'https://envinorma-back-office.herokuapp.com'


@dataclass
class _AMDifferences:
    am_id: str
    am_nor: Optional[str]
    modification_date: date
    diff: TextDifferences
    nb_modified_lines: int = field(init=False)

    def __post_init__(self) -> None:
        self.nb_modified_lines = self.diff.nb_modifications()

    @property
    def diff_url(self) -> str:
        date_before = self.modification_date - timedelta(days=2)
        today = date.today()
        return f'{_BO_URL}/compare/id/{self.am_id}/{date_before}/{today}'

    @property
    def diff_legifrance_envinorma_url(self) -> str:
        return f'{_BO_URL}/am/{self.am_id}/compare/legifrance'

    @property
    def diff_aida_envinorma_url(self) -> str:
        return f'{_BO_URL}/am/{self.am_id}/compare/aida'

    @property
    def modified_in_last_week(self) -> bool:
        return (date.today() - self.modification_date).days <= 7

    @property
    def modified_in_last_month(self) -> bool:
        return (date.today() - self.modification_date).days <= 28

    @property
    def row_description(self) -> List[str]:
        return [
            f'{self.am_id} / {self.am_nor}',
            f'{self.modification_date}',
            f'{self.nb_modified_lines}',
            f'{self.diff_url}',
        ]

    @property
    def block_description(self) -> str:
        return f'''
            AM {self.am_id} / {self.am_nor}
            Modifié le {self.modification_date} ({self.nb_modified_lines} modifications.)
            Différences : {self.diff_url}
            Différences envinorma <-> legifrance : {self.diff_legifrance_envinorma_url}
            Différences envinorma <-> aida : {self.diff_aida_envinorma_url}
        '''


@dataclass
class _AMSetDifferences:
    differences: List[_AMDifferences]
    not_found_texts: int

    def __post_init__(self):
        self.differences.sort(key=lambda x: x.modification_date, reverse=True)


def _build_message(diffs: _AMSetDifferences) -> str:
    nb_ams = diffs.not_found_texts + len(diffs.differences)
    titles = ['*AM differences*', f'{diffs.not_found_texts}/{nb_ams} AM non trouvés']
    rows = [diff.block_description for diff in diffs.differences if diff.modified_in_last_month]
    return '\n\n'.join(titles + rows)


def _build_short_message(diffs: _AMSetDifferences) -> str:
    headers = [
        'AM CID / NOR',
        'Date de dernière modification',
        'Nb lignes modifiées',
        'Différences',
    ]
    rows = [diff.row_description for diff in diffs.differences if diff.modified_in_last_month]
    return tabulate([headers] + rows)


def _send_slack_message(message: str) -> None:
    url = AM_SLACK_URL
    answer = requests.post(url, json={'text': message})
    if not (200 <= answer.status_code < 300):
        print('Error with status code', answer.status_code)
        print(answer.content.decode())


def _dispatch_to_stdout(diffs: _AMSetDifferences) -> None:
    message = _build_message(diffs)
    print(message)


def _dispatch_to_slack(diffs: _AMSetDifferences) -> None:
    message = _build_message(diffs)
    _send_slack_message(message)


def _dispatch_to_email(diffs: _AMSetDifferences) -> None:
    raise NotImplementedError


def _dispatch_to_ovh(diffs: _AMSetDifferences) -> None:
    raise NotImplementedError


def _dispatch_diffs(diffs: _AMSetDifferences) -> None:
    _dispatch_to_stdout(diffs)
    _dispatch_to_slack(diffs)
    # _dispatch_to_email(diffs)
    # _dispatch_to_ovh(diffs)


def _fetch_legifrance_text(cid: str, date_: Optional[date] = None) -> LegifranceText:
    date_ = date_ or date.today()
    client = get_legifrance_client()
    return LegifranceText.from_dict(client.consult_law_decree(cid, datetime.combine(date_, time())))


def _fetch_current_legifrance_text(cid: str) -> LegifranceText:
    return _fetch_legifrance_text(cid)


def _extract_diff(version_before: LegifranceText, version_after: LegifranceText) -> TextDifferences:
    return text_differences(version_before.extract_lines(False), version_after.extract_lines(False))


def _compute_legifrance_diff(cid: str, current_version: LegifranceText) -> TextDifferences:
    modification_date = current_version.last_modification_date - timedelta(days=2)
    version_before_last_modification = _fetch_legifrance_text(cid, modification_date)
    return _extract_diff(version_before_last_modification, current_version)


def _compute_am_diff(am_md: AMMetadata) -> Optional[_AMDifferences]:
    try:
        current_legifrance_version = _fetch_current_legifrance_text(am_md.cid)
        diff = _compute_legifrance_diff(am_md.cid, current_legifrance_version)
        return _AMDifferences(am_md.cid, am_md.nor, current_legifrance_version.last_modification_date, diff)
    except LegifranceRequestError as exc:
        print(f'Legifrance Error for am_id {am_md.cid}: {exc}')
        return None


def _compute_diffs() -> _AMSetDifferences:
    am_list = list(DATA_FETCHER.load_all_am_metadata().values())
    candidates = [
        _compute_am_diff(am_md) for am_md in typed_tqdm(am_list[:4], 'Computing diffs')
    ]  # TODO: remove restriction
    print("Computed diff.")
    am_diff = [candidate for candidate in candidates if candidate]
    nb_not_found_am = len([candidate for candidate in candidates if not candidate])
    return _AMSetDifferences(am_diff, nb_not_found_am)


def compute_and_dispatch_diff() -> None:
    diffs = _compute_diffs()
    _dispatch_diffs(diffs)


if __name__ == '__main__':
    compute_and_dispatch_diff()
