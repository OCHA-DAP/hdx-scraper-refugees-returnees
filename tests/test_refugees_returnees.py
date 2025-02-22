import filecmp
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.refugees_returnees.refugees_returnees import RefugeesReturnees


class TestRefugeesReturnees:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def read_dataset(self, monkeypatch):
        def read_from_hdx(dataset_name):
            return Dataset.load_from_json(
                join(
                    "tests",
                    "fixtures",
                    "input",
                    f"dataset-{dataset_name}.json",
                )
            )

        monkeypatch.setattr(Dataset, "read_from_hdx", staticmethod(read_from_hdx))

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "refugees_returnees", "config")

    def test_refugees_returnees(
        self,
        configuration,
        read_dataset,
        fixtures_dir,
        input_dir,
        config_dir,
    ):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "Test-refugees_returnees",
                delete_on_success=True,
                delete_on_failure=False,
            ) as tempdir:
                with Download(user_agent="test") as downloader:
                    retriever = Retrieve(
                        downloader=downloader,
                        fallback_dir=tempdir,
                        saved_dir=input_dir,
                        temp_dir=tempdir,
                        save=False,
                        use_saved=True,
                    )
                    ref_ret = RefugeesReturnees(configuration, retriever, error_handler)
                    dataset_types = ref_ret.get_data()
                    assert dataset_types == ["refugees", "returnees"]

                    dataset = ref_ret.generate_dataset("returnees")
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )
                    assert dataset == {
                        "name": "hdx-hapi-returnees",
                        "title": "HDX HAPI - Affected People: Returnees",
                        "dataset_date": "[2020-01-01T00:00:00 TO 2023-12-31T23:59:59]",
                        "tags": [
                            {
                                "name": "returnees",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            }
                        ],
                        "groups": [{"name": "world"}],
                        "license_id": "cc-by",
                        "methodology": "Registry",
                        "caveats": "HDX HAPI is refreshed daily, but the source datasets "
                        "may have different update schedules. Please refer to the source "
                        "datasets for each subcategory to verify their specific update "
                        "frequency.",
                        "dataset_source": "UNHCR - The UN Refugee Agency",
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                        "owner_org": "hdx-hapi",
                        "data_update_frequency": 1,
                        "notes": "This dataset contains data obtained from the [HDX "
                        "Humanitarian API](https://hapi.humdata.org/) (HDX HAPI), which "
                        "provides standardized humanitarian indicators designed for "
                        "seamless interoperability from multiple sources. The data "
                        "facilitates automated workflows and visualizations to support "
                        "humanitarian decision making. For more information, please see "
                        "the HDX HAPI [landing page](https://data.humdata.org/hapi) and "
                        "[documentation](https://hdx-hapi.readthedocs.io/en/latest/).\n",
                        "subnational": "0",
                        "dataset_preview": "no_preview",
                    }

                    resources = dataset.get_resources()

                    assert resources == [
                        {
                            "name": "Global Affected People: Returnees",
                            "description": "Returnees data from HDX HAPI, please see "
                            "[the documentation](https://hdx-hapi.readthedocs.io/en/"
                            "latest/data_usage_guides/affected_people/#returnees) for "
                            "more information",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        }
                    ]

                    assert filecmp.cmp(
                        join(tempdir, "hdx_hapi_returnees_global.csv"),
                        join(fixtures_dir, "hdx_hapi_returnees_global.csv"),
                    )

                    dataset = ref_ret.generate_dataset("refugees")
                    dataset.update_from_yaml(
                        path=join(config_dir, "hdx_dataset_static.yaml")
                    )
                    assert dataset == {
                        "name": "hdx-hapi-refugees",
                        "title": "HDX HAPI - Affected People: Refugees & Persons of "
                        "Concern",
                        "dataset_date": "[2020-01-01T00:00:00 TO 2023-12-31T23:59:59]",
                        "tags": [
                            {
                                "name": "refugees",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            }
                        ],
                        "groups": [{"name": "world"}],
                        "license_id": "cc-by",
                        "methodology": "Registry",
                        "caveats": "HDX HAPI is refreshed daily, but the source datasets "
                        "may have different update schedules. Please refer to the source "
                        "datasets for each subcategory to verify their specific update "
                        "frequency.",
                        "dataset_source": "UNHCR - The UN Refugee Agency",
                        "package_creator": "HDX Data Systems Team",
                        "private": False,
                        "maintainer": "aa13de36-28c5-47a7-8d0b-6d7c754ba8c8",
                        "owner_org": "hdx-hapi",
                        "data_update_frequency": 1,
                        "notes": "This dataset contains data obtained from the [HDX "
                        "Humanitarian API](https://hapi.humdata.org/) (HDX HAPI), which "
                        "provides standardized humanitarian indicators designed for "
                        "seamless interoperability from multiple sources. The data "
                        "facilitates automated workflows and visualizations to support "
                        "humanitarian decision making. For more information, please see "
                        "the HDX HAPI [landing page](https://data.humdata.org/hapi) and "
                        "[documentation](https://hdx-hapi.readthedocs.io/en/latest/).\n",
                        "subnational": "0",
                        "dataset_preview": "no_preview",
                    }

                    resources = dataset.get_resources()
                    assert resources == [
                        {
                            "name": "Global Affected People: Refugees & Persons of "
                            "Concern (2020-2024)",
                            "description": "Refugees and Persons of Concern data (2020-"
                            "2024) from HDX HAPI, please see [the documentation]"
                            "(https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides"
                            "/affected_people/#refugees-persons-of-concern) for more "
                            "information",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        }
                    ]
