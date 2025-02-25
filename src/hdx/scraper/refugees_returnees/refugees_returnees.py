#!/usr/bin/python
"""refugees_returnees scraper"""

import logging
from copy import deepcopy
from typing import Dict, List, Tuple

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve
from pandas import read_csv

logger = logging.getLogger(__name__)


REFUGEE_POPULATION_GROUPS = [
    "REF",
    "ROC",
    "ASY",
    "OIP",
    "IOC",
    "STA",
    "OOC",
    "HST",
    "RST",
    "NAT",
]

RETURNEE_POPULATION_GROUPS = [
    "RET",
    "RDP",
    "RRI",
]


class RefugeesReturnees:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        error_handler: HDXErrorHandler,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._error_handler = error_handler
        self._temp_dir = retriever.temp_dir
        self.data = {
            "returnees": [],
            "refugees": [],
        }
        self.years = {
            "returnees": [],
            "refugees": [],
        }

    def get_data(self) -> List[str]:
        dataset = Dataset.read_from_hdx(self._configuration["source_dataset"])
        resource = [
            r
            for r in dataset.get_resources()
            if r["name"] == self._configuration["source_resource"]
        ]
        resource = resource[0]
        dataset_id = dataset["id"]
        resource_id = resource["id"]

        file_path = self._retriever.download_file(resource["url"])
        contents = read_csv(file_path, skiprows=[1])
        headers = list(contents.columns)
        population_headers = [
            h for h in headers if h.split(" ")[0] in ["Female", "Male", "Total"]
        ]
        group_headers = [
            "Year",
            "Country of Origin Code",
            "Country of Asylum Code",
            "Population Type",
        ]
        column_dict = {group_header: "first" for group_header in group_headers}
        for population_header in population_headers:
            column_dict[population_header] = "sum"
        summed = contents.groupby(group_headers, as_index=False).agg(column_dict)

        rows = summed.to_dict(orient="records")
        hrps = {}
        ghos = {}
        for row in rows:
            error = None
            missing_locations = []
            origin_location_code = row["Country of Origin Code"]
            origin_hrp, origin_gho = get_hrp_gho(
                origin_location_code, hrps, ghos, missing_locations
            )

            asylum_location_code = row["Country of Asylum Code"]
            asylum_hrp, asylum_gho = get_hrp_gho(
                asylum_location_code, hrps, ghos, missing_locations
            )

            if len(missing_locations) > 0:
                for missing_location in missing_locations:
                    self._error_handler.add_message(
                        "RefugeesReturnees",
                        dataset["name"],
                        f"Could not find iso code {missing_location}",
                    )
                error = f"Non matching country code(s) {','.join(set(missing_locations))}"

            year = row["Year"]
            start_date, end_date = parse_date_range(str(year))
            start_date = iso_string_from_datetime(start_date)
            end_date = iso_string_from_datetime(end_date)

            population_group = row["Population Type"]
            data_type = None
            if population_group in REFUGEE_POPULATION_GROUPS:
                data_type = "refugees"
            if population_group in RETURNEE_POPULATION_GROUPS:
                data_type = "returnees"
            if not data_type:
                self._error_handler.add_missing_value_message(
                    "RefugeesReturnees",
                    dataset["name"],
                    "Population group",
                    population_group,
                    message_type="warning",
                )
                continue

            dict_of_lists_add(self.years, data_type, year)

            for population_header in population_headers:
                if "unknown" in population_header.lower():
                    continue
                gender, age_range = get_gender_and_age_range(population_header)
                min_age, max_age = get_min_and_max_age(age_range)

                new_row = {
                    "origin_location_code": origin_location_code,
                    "origin_has_hrp": origin_hrp,
                    "origin_in_gho": origin_gho,
                    "asylum_location_code": asylum_location_code,
                    "asylum_has_hrp": asylum_hrp,
                    "asylum_in_gho": asylum_gho,
                    "population_group": population_group,
                    "gender": gender,
                    "age_range": age_range,
                    "min_age": min_age,
                    "max_age": max_age,
                    "population": row[population_header],
                    "reference_period_start": start_date,
                    "reference_period_end": end_date,
                    "dataset_hdx_id": dataset_id,
                    "resource_hdx_id": resource_id,
                    "warning": None,
                    "error": error,
                    "year": year,
                }
                dict_of_lists_add(self.data, data_type, new_row)

        return sorted(list(self.data.keys()))

    def generate_dataset(self, data_type: str) -> Dataset:
        dataset = Dataset(
            {
                "name": self._configuration["output_datasets"][data_type]["name"],
                "title": self._configuration["output_datasets"][data_type]["title"],
            }
        )
        year_start = min(self.years[data_type])
        year_end = max(self.years[data_type])
        dataset.set_time_period_year_range(year_start, year_end)

        tags = self._configuration["tags"][data_type]
        dataset.add_tags(tags)

        dataset.add_other_location("world")

        hxl_tags = self._configuration["hxl_tags"]
        headers = list(hxl_tags.keys())

        if data_type == "returnees":
            dataset.generate_resource_from_iterable(
                headers,
                self.data[data_type],
                hxl_tags,
                self._temp_dir,
                f"hdx_hapi_{data_type}_global.csv",
                self._configuration["resources"][data_type],
                encoding="utf-8-sig",
            )
            return dataset

        # break up refugees data
        start_year = year_start - year_start % 5
        for sy in reversed(range(start_year, year_end + 1, 5)):
            ey = sy + 4
            year_range = f"{sy}-{ey}"

            resource_data = deepcopy(self._configuration["resources"][data_type])
            resource_data["name"] = resource_data["name"].replace("YYYY", year_range)
            resource_data["description"] = resource_data["description"].replace(
                "YYYY", year_range
            )
            filename = f"hdx_hapi_{data_type}_global_{year_range.replace('-', '_')}.csv"
            rows = [r for r in self.data[data_type] if sy <= r["year"] <= ey]
            dataset.generate_resource_from_iterable(
                headers,
                rows,
                hxl_tags,
                self._temp_dir,
                filename,
                resource_data,
                encoding="utf-8-sig",
            )

        return dataset


def get_hrp_gho(
    iso: str, hrps: Dict[str, str], ghos: Dict[str, str], missing_locations: List[str]
) -> Tuple[str, str]:
    values = {True: "Y", False: "N"}
    hrp = hrps.get(iso)
    if hrp is None:
        hrp = Country.get_hrp_status_from_iso3(iso)
        hrp = values.get(hrp)
        if hrp is None:
            missing_locations.append(iso)
        hrps[iso] = hrp
    gho = ghos.get(iso)
    if gho is None:
        gho = Country.get_gho_status_from_iso3(iso)
        gho = values.get(gho)
        ghos[iso] = gho
    return hrp, gho


def get_gender_and_age_range(header: str) -> (str, str):
    header = header.lower()
    gender = "all"
    age_range = "all"

    if header.startswith("female"):
        gender = "f"
    if header.startswith("male"):
        gender = "m"

    split_header = header.split(" ")
    if len(split_header) == 1:
        return gender, age_range

    age_component = " ".join(split_header[1:])
    if age_component == "total":
        return gender, age_range

    age_range = age_component
    if age_component.endswith("or more"):
        age_range = age_component.replace(" or more", "+")

    return gender, age_range


def get_min_and_max_age(age_range: str) -> (int | None, int | None):
    if age_range == "all" or age_range == "unknown":
        return None, None
    ages = age_range.split("-")
    if len(ages) == 2:
        # Format: 0-5
        min_age, max_age = int(ages[0]), int(ages[1])
    else:
        # Format: 80+
        min_age = int(age_range.replace("+", ""))
        max_age = None
    return min_age, max_age
