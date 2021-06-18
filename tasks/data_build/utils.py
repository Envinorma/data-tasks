from typing import Iterable, Optional, TypeVar

from tqdm import tqdm

T = TypeVar('T')


def typed_tqdm(
    collection: Iterable[T], desc: Optional[str] = None, leave: bool = True, disable: bool = False
) -> Iterable[T]:
    return tqdm(collection, desc=desc, leave=leave, disable=disable)
