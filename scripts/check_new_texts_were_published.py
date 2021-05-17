'''
Script for detecting differences between remote texts and envinorma texts.
'''
import pathlib
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from envinorma.data import ID_TO_AM_MD, AMSource, ArreteMinisteriel, extract_text_lines
from envinorma.structure.am_structure_extraction import transform_arrete_ministeriel
from leginorma import LegifranceClient
from leginorma.models import LegifranceText
from text_diff import AddedLine, ModifiedLine, RemovedLine, TextDifferences, UnchangedLine, text_differences
from tqdm import tqdm

from data_build.config import DATA_FETCHER, LEGIFRANCE_CLIENT_ID, LEGIFRANCE_CLIENT_SECRET


def _load_legifrance_version(am_id: str) -> ArreteMinisteriel:
    client = LegifranceClient(LEGIFRANCE_CLIENT_ID, LEGIFRANCE_CLIENT_SECRET)
    legifrance_current_version = LegifranceText.from_dict(client.consult_law_decree(am_id))
    random.seed(legifrance_current_version.title)
    return transform_arrete_ministeriel(legifrance_current_version, am_id=am_id)


def _clean_line(line: str) -> str:
    return line.replace('<br/>', '')


def _remove_empty(lines: List[str]) -> List[str]:
    return [line for line in lines if line]


def _extract_lines(am: ArreteMinisteriel) -> List[str]:
    return _remove_empty([_clean_line(line) for section in am.sections for line in extract_text_lines(section, 0)])


def _compute_am_diff(am_before: ArreteMinisteriel, am_after: ArreteMinisteriel) -> TextDifferences:
    lines_before = _extract_lines(am_before)
    lines_after = _extract_lines(am_after)
    return text_differences(lines_before, lines_after)


def _compute_modification_ratio(diff: TextDifferences) -> float:
    nb_modified_lines = len([0 for dl in diff.diff_lines if not isinstance(dl, UnchangedLine)])
    modification_ratio = nb_modified_lines / len(diff.diff_lines)
    return modification_ratio


def _seems_too_big(diff: TextDifferences) -> bool:
    return _compute_modification_ratio(diff) >= 0.03


def _am_has_changed(am_id: str) -> Tuple[bool, Optional[TextDifferences]]:
    envinorma_version = DATA_FETCHER.load_initial_am(am_id)
    if not envinorma_version:
        return False, None
    legifrance_version = _load_legifrance_version(am_id)
    diff = _compute_am_diff(envinorma_version, legifrance_version)
    if _seems_too_big(diff):
        return True, diff
    return False, diff


def _pretty_print_diff(diff: TextDifferences):
    for line in diff.diff_lines:
        if isinstance(line, UnchangedLine):
            continue
        if isinstance(line, AddedLine):
            print(f'+{line.content}')
        if isinstance(line, RemovedLine):
            print(f'-{line.content}')
        if isinstance(line, ModifiedLine):
            print(f'M-{line.content_before}')
            print(f'M+{line.content_after}')


def _write_diff_description(am_id_to_diff: Dict[str, TextDifferences]) -> None:
    lines = [
        line
        for am_id, diff in am_id_to_diff.items()
        for line in [am_id, f'ratio : {_compute_modification_ratio(diff)}']
    ]
    date_ = datetime.now().strftime('%Y-%m-%d-%H-%M')
    destination = pathlib.Path(__file__).parent.parent.joinpath(f'data/legifrance_diffs/{date_}.txt')
    open(destination, 'w').write('\n'.join(lines))


def run() -> None:
    changed = {}
    for am_id, md in tqdm(ID_TO_AM_MD.items()):
        if md.source != AMSource.LEGIFRANCE:
            continue
        am_id = str(am_id)
        try:
            am_has_changed, diff = _am_has_changed(am_id)
        except Exception as exc:
            print(am_id, str(exc))
            continue
        if am_has_changed:
            changed[am_id] = diff
            print(f'AM {am_id} seems to have changed.')
    _write_diff_description(changed)
    for am_id, diff in changed.items():
        print(am_id)
        _pretty_print_diff(diff)


def _dump_before_after(am_id: str):
    envinorma_version = DATA_FETCHER.load_initial_am(am_id)
    if not envinorma_version:
        return
    legifrance_version = _load_legifrance_version(am_id)
    open('tmp_before.txt', 'w').write('\n'.join(_extract_lines(envinorma_version)))
    open('tmp_after.txt', 'w').write('\n'.join(_extract_lines(legifrance_version)))


if __name__ == '__main__':
    run()
