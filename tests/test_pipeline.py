from os.path import join

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.refugees_returnees.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(
        self,
        configuration,
        read_dataset,
        fixtures_dir,
        input_dir,
        config_dir,
    ):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "Test_refugees_returnees",
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

                    pipeline = Pipeline(configuration, retriever, error_handler)
                    dataset_types = pipeline.get_data()
                    assert dataset_types == ["refugees", "returnees"]

                    dataset = pipeline.generate_dataset("returnees")
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

                    assert_files_same(
                        join(tempdir, "hdx_hapi_returnees_global.csv"),
                        join(fixtures_dir, "hdx_hapi_returnees_global.csv"),
                    )

                    dataset = pipeline.generate_dataset("refugees")
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
