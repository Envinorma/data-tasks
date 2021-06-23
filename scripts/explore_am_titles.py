"""Explorations in preparation of the new iteration of topic detection.
The idea here is to find the topics that are most generic topics.
"""

import random
from collections import Counter
from typing import Dict, List, Optional, Union

from envinorma.models.arrete_ministeriel import ArreteMinisteriel
from envinorma.models.structured_text import StructuredText

from tasks.data_build.load import load_ams

_Section = Union[StructuredText, ArreteMinisteriel]


def _print_titles(
    section: _Section, depth: int = 0, max_depth: Optional[int] = None, target_depth: Optional[int] = None
) -> None:
    if target_depth is None or depth == target_depth:
        print('\t' * depth + section.title.text)
    if max_depth is None or depth < max_depth:
        for subsection in section.sections:
            _print_titles(subsection, depth + 1, max_depth, target_depth)


def _match(section_title: str) -> bool:
    return 'disposition' in section_title.lower() and 'rale' in section_title.lower()


def _detect_matching_depth(section: _Section) -> Optional[int]:
    if _match(section.title.text):
        return 0
    optional_child_matching_depths = [_detect_matching_depth(child) for child in section.sections]
    child_matching_depths = [depth for depth in optional_child_matching_depths if depth is not None]
    return None if not child_matching_depths else 1 + min(child_matching_depths)


def _count_matching_depths(ams: List[ArreteMinisteriel]) -> Dict[Optional[int], int]:
    return Counter([_detect_matching_depth(am) for am in ams])


def _print_non_matching_am_ids(ams: List[ArreteMinisteriel]) -> None:
    for am in ams:
        if _detect_matching_depth(am) is None:
            print(am.id)


def _print_matching_depth(am: ArreteMinisteriel) -> None:
    matching_depth = _detect_matching_depth(am)
    print(f'AM {am.id}')
    if matching_depth is None:
        print('No matching depth.')
        return
    _print_titles(am, target_depth=matching_depth)


def _print_all_matching_depths(ams: List[ArreteMinisteriel], target_depth: int) -> None:
    for am in ams:
        depth = _detect_matching_depth(am)
        if depth == target_depth:
            _print_matching_depth(am)


if __name__ == '__main__':
    _AMS = list(load_ams().values())
    am = random.choice(_AMS)
    _print_titles(am, 2)

    print(_count_matching_depths(_AMS))
    _print_non_matching_am_ids(_AMS)
    _print_all_matching_depths(_AMS, 0)
