#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file, temp_dir_batch
from hdx.utilities.retriever import Retrieve

from hdx.scraper.refugees_returnees.refugees_returnees import RefugeesReturnees

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-refugees-returnees"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: refugees-returnees"


def main(
    save: bool = True,
    use_saved: bool = False,
    err_to_hdx: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (bool): Whether to write any errors to HDX metadata. Defaults to False.

    Returns:
        None
    """
    configuration = Configuration.read()
    if not User.check_current_user_organization_access("hdx", "create_dataset"):
        raise PermissionError("API Token does not give access to HDX organisation!")

    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with temp_dir_batch(folder=_USER_AGENT_LOOKUP) as info:
            temp_dir = info["folder"]
            with Download() as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=temp_dir,
                    saved_dir=_SAVED_DATA_DIR,
                    temp_dir=temp_dir,
                    save=save,
                    use_saved=use_saved,
                )
                ref_ret = RefugeesReturnees(configuration, retriever, error_handler)
                dataset_types = ref_ret.get_data()

                for dataset_type in dataset_types:
                    dataset = ref_ret.generate_dataset(dataset_type)
                    dataset.update_from_yaml(
                        path=script_dir_plus_file(
                            join(
                                "config",
                                "hdx_dataset_static.yaml",
                            )
                        )
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        match_resource_order=False,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
