from data_build.build.build_am_repository import _group_by_key


def test_group_by_key():
    assert _group_by_key([], '') == {}
    assert _group_by_key([{'a': 1}], 'a') == {1: [{'a': 1}]}
    assert _group_by_key([{'a': 1}, {'a': 1}], 'a') == {1: [{'a': 1}, {'a': 1}]}
    assert _group_by_key([{'a': 1}, {'a': 2}], 'a') == {1: [{'a': 1}], 2: [{'a': 2}]}
    assert _group_by_key([{'a': 1}, {'a': 2}, {'a': 1, 'c': 2}], 'a') == {
        1: [{'a': 1}, {'a': 1, 'c': 2}],
        2: [{'a': 2}],
    }
