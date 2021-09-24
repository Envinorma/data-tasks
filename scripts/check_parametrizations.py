# Check that all parameters cannot be satisfied simultaneously.

from tasks.data_build.config import DATA_FETCHER


def run() -> None:
    res = DATA_FETCHER.build_enriched_ams()

    for am in res:
        for sec in am.descendent_sections():
            if not sec.parametrization_elements_are_compatible():
                print(am.id, sec.title.text)


if __name__ == "__main__":
    run()
