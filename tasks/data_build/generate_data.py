'''
Download last versions of AM and send them to envinorma-web
'''

from .build.build_am_repository import generate_am_repository  # noqa: F401
from .build.build_ams import generate_ams  # noqa: F401
from .build.build_aps import dump_ap_datasets, dump_aps  # noqa: F401
from .build.build_classements import build_all_classement_datasets, build_classements_csv  # noqa: F401
from .build.build_documents import build_all_document_datasets, download_georisques_documents  # noqa: F401
from .build.build_georisques_ids import dump_georisques_ids  # noqa: F401
from .build.build_installations import build_all_installations_datasets, build_installations_csv  # noqa: F401
from .validate.check_am import check_ams
from .validate.check_classements import check_classements_csv
from .validate.check_documents import check_documents_csv
from .validate.check_installations import check_installations_csv


def run():
    generate_ams()
    # generate_am_repository()
    # build_installations_csv()
    # build_all_installations_datasets()
    # build_classements_csv()
    # build_all_classement_datasets()
    # download_georisques_documents()
    # build_all_document_datasets()
    # dump_aps('sample')
    # dump_ap_datasets()
    # dump_georisques_ids(filename)

    check_classements_csv()
    check_installations_csv()
    check_documents_csv()
    check_ams()


if __name__ == '__main__':
    run()
