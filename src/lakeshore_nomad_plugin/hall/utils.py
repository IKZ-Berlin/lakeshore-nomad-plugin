#
# Copyright The NOMAD Authors.
#
# This file is part of NOMAD. See https://nomad-lab.eu for further info.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Utility functions for the NeXus reader classes."""

from dataclasses import dataclass, replace
from typing import List, Any, Dict, Optional, Tuple, Union, Generator
from collections.abc import Mapping
import json
import yaml
import re
import math
from datetime import datetime
import numpy as np
import pandas as pd
import pytz

from nomad.units import ureg

from lakeshore_nomad_plugin.hall import instrument as hall_instrument
from lakeshore_nomad_plugin.hall.measurement import (
    Measurement,
    VariableTemperatureMeasurement,
    VariableTemperatureResults,
    VariableFieldMeasurement,
    VariableFieldResults,
    IVCurveMeasurement,
    IVResults,
)


@dataclass
class FlattenSettings:
    """Settings for flattening operations.

    Args:
        dic (dict): Dictionary to flatten
        convert_dict (dict): Dictionary for renaming keys in the flattend dict.
        replace_nested (dict): Dictionary for renaming nested keys.
        parent_key (str, optional):
            Parent key of the dictionary. Defaults to "/ENTRY[entry]".
        sep (str, optional): Separator for the keys. Defaults to "/".
    """

    dic: Mapping
    convert_dict: dict
    replace_nested: dict
    parent_key: str = "/ENTRY[entry]"
    sep: str = "/"
    is_in_section: bool = False


def get_reference(upload_id, entry_id):
    return f"../uploads/{upload_id}/archive/{entry_id}"


def get_entry_id(upload_id, filename):
    from nomad.utils import hash

    return hash(upload_id, filename)


def get_hash_ref(upload_id, filename):
    return f"{get_reference(upload_id, get_entry_id(upload_id, filename))}#data"


def nan_equal(a, b):
    """
    Compare two values with NaN values.
    """
    if isinstance(a, float) and isinstance(b, float):
        return a == b or (math.isnan(a) and math.isnan(b))
    elif isinstance(a, dict) and isinstance(b, dict):
        return dict_nan_equal(a, b)
    elif isinstance(a, list) and isinstance(b, list):
        return list_nan_equal(a, b)
    else:
        return a == b


def list_nan_equal(list1, list2):
    """
    Compare two lists with NaN values.
    """
    if len(list1) != len(list2):
        return False
    for a, b in zip(list1, list2):
        if not nan_equal(a, b):
            return False
    return True


def dict_nan_equal(dict1, dict2):
    """
    Compare two dictionaries with NaN values.
    """
    if set(dict1.keys()) != set(dict2.keys()):
        return False
    for key in dict1:
        if not nan_equal(dict1[key], dict2[key]):
            return False
    return True


def create_archive(
    entry_dict, context, filename, file_type, logger, *, overwrite: bool = False
):
    from nomad.datamodel.context import ClientContext
    from nomad.datamodel import EntryArchive

    file_exists = context.raw_path_exists(filename)
    dicts_are_equal = None
    if isinstance(context, ClientContext):
        return None
    if file_exists:
        with context.raw_file(filename, "r") as file:
            existing_dict = yaml.safe_load(file)
            dicts_are_equal = dict_nan_equal(existing_dict, entry_dict)
    if not file_exists or overwrite or dicts_are_equal:
        with context.raw_file(filename, "w") as newfile:
            if file_type == "json":
                json.dump(entry_dict, newfile)
            elif file_type == "yaml":
                yaml.dump(entry_dict, newfile)
        context.upload.process_updated_raw_file(filename, allow_modify=True)
    elif file_exists and not overwrite and not dicts_are_equal:
        logger.error(
            f"{filename} archive file already exists. "
            f"You are trying to overwrite it with a different content. "
            f"To do so, remove the existing archive and click reprocess again."
        )
    return get_hash_ref(context.upload_id, filename)


def is_section(val: Any) -> bool:
    """Checks whether a value is a section.

    Args:
        val (Any): A list or value.

    Returns:
        bool: True if val is a section.
    """
    return isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict)


def is_value_unit_pair(val: Any) -> bool:
    """Checks whether the value contains a dict of a value unit pair.

    Args:
        val (Any): The value to be checked.

    Returns:
        bool: True if val contains a value unit pair dict.
    """
    if not isinstance(val, dict):
        return False

    if len(val) == 2 and "value" in val and "unit" in val:
        return True
    return False


def uniquify_keys(ldic: list) -> List[Any]:
    """Uniquifys keys in a list of tuple lists containing key value pairs.

    Args:
        ldic (list): List of lists of length two, containing key value pairs.

    Returns:
        List[Any]: Uniquified list, where duplicate keys are appended with 1, 2, etc.
    """
    dic: dict = {}
    for key, val in ldic:
        suffix = 0
        sstr = "" if suffix == 0 else str(suffix)
        while f"{key}{sstr}" in dic:
            sstr = "" if suffix == 0 else str(suffix)
            suffix += 1

        if is_value_unit_pair(val):
            dic[f"{key}{sstr}"] = val["value"]
            dic[f"{key}{sstr}/@units"] = val["unit"]
            continue
        dic[f"{key}{sstr}"] = val

    return list(map(list, dic.items()))


def parse_section(key: str, val: Any, settings: FlattenSettings) -> List[Any]:
    """Parse a section, i.e. an entry containing a list of entries.

    Args:
        key (str): The key which is currently being checked.
        val (Any): The value at the current key.
        settings (FlattenSettings): The flattening settings.

    Returns:
        List[Any]: A list of list tuples containing key, value pairs.
    """
    if not is_section(val):
        return [(key, val)]

    groups: List[Any] = []
    for group in val:
        groups.extend(
            flatten_and_replace(
                replace(settings, dic=group, parent_key=key, is_in_section=True)
            ).items()
        )

    return uniquify_keys(groups)


def flatten_and_replace(settings: FlattenSettings) -> dict:
    """Flatten a nested dictionary, and replace the keys with the appropriate
    paths in the nxs file.

    Args:
        settings (FlattenSettings): Settings dataclass for flattening the data.

    Returns:
        dict: Flattened dictionary
    """
    items: List[Any] = []
    for key, val in settings.dic.items():
        new_key = (
            settings.parent_key + settings.sep + settings.convert_dict.get(key, key)
        )
        if isinstance(val, Mapping):
            items.extend(
                flatten_and_replace(
                    replace(settings, dic=val, parent_key=new_key)
                ).items()
                if not (settings.is_in_section and is_value_unit_pair(val))
                else [[new_key, val]]
            )
            continue

        for old, new in settings.replace_nested.items():
            new_key = new_key.replace(old, new)

        if new_key.endswith("/value"):
            items.append((new_key[:-6], val))
        else:
            items.extend(parse_section(new_key, val, settings))

    return dict(items)


def parse_yml(
    file_path: str,
    convert_dict: Optional[dict] = None,
    replace_nested: Optional[dict] = None,
) -> Dict[str, Any]:
    """Parses a metadata yaml file into a dictionary.

    Args:
        file_path (str): The file path of the yml file.

    Returns:
        Dict[str, Any]: The dictionary containing the data readout from the yml.
    """
    if convert_dict is None:
        convert_dict = {}

    if replace_nested is None:
        replace_nested = {}

    convert_dict["unit"] = "@units"

    with open(file_path, encoding="utf-8") as file:
        return flatten_and_replace(
            FlattenSettings(
                dic=yaml.safe_load(file),
                convert_dict=convert_dict,
                replace_nested=replace_nested,
            )
        )


def parse_json(file_path: str) -> Dict[str, Any]:
    """Parses a metadata json file into a dictionary.

    Args:
        file_path (str): The file path of the json file.

    Returns:
        Dict[str, Any]: The dictionary containing the data readout from the json.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def is_activity_section(section):
    return any("Activity" in i.label for i in section.m_def.all_base_sections)


def handle_section(section):
    from nomad.datamodel.metainfo.basesections import ExperimentStep

    if hasattr(section, "reference") and is_activity_section(section.reference):
        return [ExperimentStep(activity=section.reference, name=section.reference.name)]
    if section.m_def.label == "CharacterizationMovpe":
        sub_sect_list = []
        for sub_section in vars(section).values():
            if isinstance(sub_section, list):
                for item in sub_section:
                    if hasattr(item, "reference") and is_activity_section(
                        item.reference
                    ):
                        sub_sect_list.append(
                            ExperimentStep(
                                activity=item.reference, name=item.reference.name
                            )
                        )
        return sub_sect_list
    if not hasattr(section, "reference") and is_activity_section(section):
        return [ExperimentStep(activity=section, name=section.name)]


def has_section_format(expr: str) -> bool:
    """Checks whether an expression follows the form of a section
    i.e. is of the form [section]

    Args:
        expr (str): The current expression to check

    Returns:
        bool: Returns true if the expr is of the form of a section
    """
    return bool(re.search(r"^\[.+\]$", expr))


def is_measurement(expr):
    """Checks whether an expression follows the form of a measurement indicator
    i.e. is of the form <measurement>

    Args:
        expr (str): The current expression to check

    Returns:
        bool: Returns true if the expr is of the form of a measurement indicator
    """
    return bool(re.search(r"^\<.+\>$", expr))


def is_key(expr: str) -> bool:
    """Checks whether an expression follows the form of a key value pair
    i.e. is of the form key: value or key = value

    Args:
        expr (str): The current expression to check

    Returns:
        bool: Returns true if the expr is of the form of a key value pair
    """
    return bool(re.search(r"^.+\s*[:|=]\s*.+$", expr))


def is_meas_header(expr: str) -> bool:
    """Checks whether an expression follows the form of a measurement header,
    i.e. starts with: Word [Unit]

    Args:
        expr (str): The current expression to check

    Returns:
        bool: Returns true if the expr is of the form of a measurement header
    """
    return bool(re.search(r"^[^\]]+\[[^\]]+\]", expr))


def is_value_with_unit(expr: str) -> bool:
    """Checks whether an expression is a value with a unit,
    i.e. is of the form value [unit].

    Args:
        expr (str): The expression to check

    Returns:
        bool: Returns true if the expr is a value with unit
    """
    return bool(re.search(r"^.+\s\[.+\]$", expr))


def is_integer(expr: str) -> bool:
    """Checks whether an expression is an integer number,
    i.e. 3, +3 or -3. Also supports numbers in the form of 003.

    Args:
        expr (str): The expression to check

    Returns:
        bool: Returns true if the expr is an integer number
    """
    return bool(re.search(r"^[+-]?\d+$", expr))


def is_number(expr: str) -> bool:
    """Checks whether an expression is a number,
    i.e. is of the form 0.3, 3, 1e-3, 1E5 etc.

    Args:
        expr (str): The expression to check

    Returns:
        bool: Returns true if the expr is a number
    """
    return bool(
        re.search(r"^[+-]?(\d+([.]\d*)?([eE][+-]?\d+)?|[.]\d+([eE][+-]?\d+)?)$", expr)
    )


def is_boolean(expr: str) -> bool:
    """Checks whether an expression is a boolean,
    i.e. is equal to True or False (upper or lower case).

    Args:
        expr (str): The expression to check.

    Returns:
        bool: Returns true if the expr is a boolean
    """
    return bool(re.search(r"True|False|true|false|On|Off|Yes|No", expr))


def to_bool(expr: str) -> bool:
    """Converts boolean representations in strings to python booleans.

    Args:
        expr (str): The string to convert to boolean.

    Returns:
        bool: The boolean value.
    """
    replacements = {
        "On": True,
        "Off": False,
        "Yes": True,
        "No": False,
        "True": True,
        "False": False,
        "true": True,
        "false": False,
    }

    return replacements.get(expr)


def split_str_with_unit(expr: str, lower: bool = True) -> Tuple[str, str]:
    """Splits an expression into a string and a unit.
    The input expression should be of the form value [unit] as
    is checked with is_value_with_unit function.

    Args:
        expr (str): The expression to split
        lower (bool, optional):
            If True the value is converted to lower case. Defaults to True.

    Returns:
        Tuple[str, str]: A tuple of a value unit pair.
    """
    value = re.split(r"\s+\[.+\]", expr)[0]
    unit = re.search(r"(?<=\[).+?(?=\])", expr)[0]

    if lower:
        return value.lower(), unit
    return value, unit


def split_value_with_unit(expr: str) -> Tuple[Union[float, str], str]:
    """Splits an expression into a string or float and a unit.
    The input expression should be of the form value [unit] as
    is checked with is_value_with_unit function.
    The value is automatically converted to a float if it is a number.

    Args:
        expr (str): The expression to split

    Returns:
        Tuple[Union[float, str], str]: A tuple of a value unit pair.
    """
    value, unit = split_str_with_unit(expr, False)

    if is_number(value):
        return float(value), unit

    return value, unit


def clean(unit: str) -> str:
    """Cleans an unit string, e.g. converts `VS` to `volt * seconds`.
    If the unit is not in the conversion dict the input string is
    returned without modification.

    Args:
        unit (str): The dirty unit string.

    Returns:
        str: The cleaned unit string.
    """
    conversions = {
        "VS": "volt * second",
        "Sec": "s",
        "²": "^2",
        "³": "^3",
        "ohm cm": "ohm * cm",
    }

    for old, new in conversions.items():
        unit = unit.replace(old, new)

    return unit


def get_unique_dkey(dic: dict, dkey: str) -> str:
    """Checks whether a data key is already contained in a dictionary
    and returns a unique key if it is not.

    Args:
        dic (dict): The dictionary to check for keys
        dkey (str): The data key which shall be written.

    Returns:
        str: A unique data key. If a key already exists it is appended with a number
    """
    suffix = 0
    while f"{dkey}{suffix}" in dic:
        suffix += 1

    return f"{dkey}{suffix}"


def pandas_df_to_template(prefix: str, data: pd.DataFrame) -> Dict[str, Any]:
    """Converts a dataframe to a NXdata entry template.

    Args:
        prefix (str): The path prefix to write the data into. Without a trailing slash.
        df (pd.DataFrame): The dataframe which should be converted.

    Returns:
        Dict[str, Any]: The dict containing the data and metainfo.
    """
    if prefix.endswith("/"):
        prefix = prefix[:-1]

    template: Dict[str, Any] = {}
    template[f"{prefix}/@NX_class"] = "NXdata"

    def write_data(header: str, attr: str, data: np.ndarray) -> None:
        if header is None:
            print("Warning: Trying to write dataframe without a header. Skipping.")
            return

        if is_value_with_unit(header):
            name, unit = split_str_with_unit(header)
            template[f"{prefix}/{name}/@units"] = clean(unit)
        else:
            name = header.lower()

        if attr == "@auxiliary_signals":
            if f"{prefix}/{attr}" in template:
                template[f"{prefix}/{attr}"].append(name)
            else:
                template[f"{prefix}/{attr}"] = [name]
        else:
            template[f"{prefix}/{attr}"] = name
        template[f"{prefix}/{name}"] = data

    if data.index.name is None:
        data = data.set_index(data.columns[0])

    # Drop last line if it has an errornous zero temperature
    if data.index.values[-1] == 0:
        data = data.iloc[:-1]

    write_data(data.index.name, "@axes", data.index.values)
    write_data(data.columns[0], "@signal", data.iloc[:, 0].values)

    for column in data.columns[1:]:
        write_data(column, "@auxiliary_signals", data[column].values)

    return template


def convert_date(datestr: str, timezone: str = "Europe/Berlin") -> str:
    """Converts a hall date formated string to isoformat string.

    Args:
        datestr (str): The hall date string
        timezone (str): The timezone of the hall date string. Defaults to "Europe/Berlin"

    Returns:
        str: The iso formatted string.
    """

    try:
        for fmt in [r"%m/%d/%y %H%M%S", r"%d.%m.%Y %H%M%S", r"%d.%m.%Y %I%M%S %p"]:
            try:
                return (
                    datetime.strptime(datestr, fmt)
                    .astimezone(pytz.timezone(timezone))
                    .isoformat()
                )
            except ValueError:
                pass
    except ValueError:
        print("Warning: datestring does not conform to date format. Skipping.")
        return datestr


def get_measurement_object(measurement_type: str) -> Measurement:
    """
    Gets a measurement MSection object from the given measurement type.

    Args:
        measurement_type (str): The measurement type.

    Returns:
        Measurement: A MSection representing a Hall measurement.
    """
    if measurement_type == "Variable Temperature Measurement":
        return VariableTemperatureMeasurement()
    if measurement_type == "Variable Field Measurement":
        return VariableFieldMeasurement()
    if measurement_type == "IV Curve Measurement":
        return IVCurveMeasurement()
    return Measurement()


def get_data_object(measurement_type: str):
    """
    Gets a measurement data MSection object from the given measurement type.

    Args:
        measurement_type (str): The measurement type.

    Returns:
        A MSection representing a Hall measurement data object.
    """
    if measurement_type == "Variable Temperature Measurement":
        return VariableTemperatureResults()
    if measurement_type == "Variable Field Measurement":
        return VariableFieldResults()
    if measurement_type == "IV Curve Measurement":
        return IVResults()
    return None


def to_snake_case(string: str) -> str:
    """
    Convert a string to snake_case.

    Preserve all non-alphanumeric characters but dashes,
    keep not separated capitalized acronyms,
    keep not separated multi-digit numbers

    Parameters:
        string (str): The string to convert.

    Returns:
        str: The converted string in snake_case.

    Example:
        >>> to_snake_case('My_String-Dashed_LS56 Sep AC / test_ls58_/@with_unit 345')
        'my_string_dashed_ls56_sep_ac/test_ls58/@with_unit_345'
    """

    string = string.replace("-", "_")
    string = re.sub(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", string)
    string = string.lower()
    string = re.sub(r"_{2,}", "_", string)
    string = re.sub(r"\b\d+\b", lambda match: match.group(0).replace(".", "_"), string)
    string = re.sub(
        r"\b(?<!\d)[A-Z]{2,}(?!\w)", lambda match: match.group(0).lower(), string
    )
    string = string.replace(" ", "_")
    string = re.sub(r"(?<=/)_", "", string)
    string = re.sub(r"_(?=/)", "", string)
    string = re.sub(r"_{2,}", "_", string)

    return string


def split_value_unit(expr: str) -> Tuple[str, Optional[str]]:
    """
    Searches for a value unit pair and returns the values for a combination
    `value [unit]`.

    Args:
        expr (str): The expression to search for a value unit pair.

    Returns:
        Tuple[str, Optional[str]]:
            A tuple of value and unit.
            Returns the expr, where spaces are replaced with `_` and None when no
            value unit expression is found.
    """
    is_value_unit = re.search(r"([^\[]+)\s\[(.*)\]", expr)
    if is_value_unit:
        value = to_snake_case(is_value_unit.group(1))
        unit = is_value_unit.group(2)
        return value.lower(), clean(unit)
    return to_snake_case(expr), None


def rename_key(key: str) -> str:
    """
    Renames the key from the file to the eln

    Args:
        key (str): They key as read from the file.

    Returns:
        str: The key replaced with its eln counterpart
    """
    key_map = {
        "use_zero_field_resistivity_to_calculate_hall_mobility": "use_zero_field_resistivity",
        "at_field": "field_at_zero_resistivity",
        "at_temperature": "temperature_at_zero_resistivity",
    }
    return key_map.get(key, key)


def calc_best_fit_values(iv_measurement: IVResults) -> IVResults:
    """
    Calculates the best fit voltage values from the provided
    fitting data.

    Args:
        iv_measurement (IVResults): The IVResults without discrete best fit values.

    Returns:
        IVResults: The IVResults with discret best fit values
    """
    iv_measurement.best_fit_values = (
        iv_measurement.current * iv_measurement.best_fit_resistance
        + iv_measurement.best_fit_offset
    )

    return iv_measurement


def get_measurements(data_template: dict) -> Generator[Measurement, None, None]:
    """
    Returns a hall measurement MSection representation form its corresponding
    nexus data_template.

    Args:
        data_template (dict): The nomad-parser-nexus data template.

    Yields:
        Generator[Measurement, None, None]:
            A generator yielding the single hall measurements.
    """
    highest_index = 1

    for key in data_template:
        if bool(re.search(f"^/entry/measurement/{highest_index}_.+/", key)):
            highest_index += 1

    for measurement_index in range(1, highest_index):
        first = True
        data_entries: dict = {}
        contact_sets: dict = {}

        for key in data_template:
            if not key.startswith(f"/entry/measurement/{measurement_index}_"):
                continue

            if first:
                measurement_type = re.search(
                    f"measurement/{measurement_index}_([^/]+)/", key
                ).group(1)
                first = False
                eln_measurement = get_measurement_object(measurement_type)

            clean_key = to_snake_case(key.split(f"{measurement_type}/")[1])

            if "/Contact Sets/" in key:
                contact_set = re.search("/Contact Sets/([^/]+)/", key).group(1)
                if contact_set not in contact_sets:
                    contact_sets[contact_set] = get_data_object(measurement_type)
                clean_dkey = clean_key.split(f"{contact_set.lower()}/")[1]
                contact_sets[contact_set].contact_set = contact_set

                if "data0" in key:
                    data = data_template[key]

                    for column in data.columns:
                        if (data[column] == "ERROR").all():
                            continue
                        if data[column].isna().all():
                            continue
                        col, unit = split_value_unit(column)
                        clean_col = col.lower().replace(" ", "_")
                        if hasattr(contact_sets[contact_set], clean_col):
                            if unit is not None:
                                setattr(
                                    contact_sets[contact_set],
                                    clean_col,
                                    pd.to_numeric(
                                        data[column], errors="coerce"
                                    )  # data[column].astype(np.float64)
                                    * ureg(unit),
                                )
                            else:
                                setattr(
                                    contact_sets[contact_set],
                                    clean_col,
                                    data[column],
                                )
                    continue

                clean_dkey, unit = split_value_unit(key.split(f"{contact_set}/")[1])
                if hasattr(contact_sets[contact_set], clean_dkey):
                    if unit is not None:
                        setattr(
                            contact_sets[contact_set],
                            clean_dkey,
                            data_template[key] * ureg(unit),
                        )
                    elif f"{key}/@units" in data_template:
                        setattr(
                            contact_sets[contact_set],
                            clean_dkey,
                            data_template[key] * ureg(data_template[f"{key}/@units"]),
                        )
                    else:
                        setattr(
                            contact_sets[contact_set],
                            clean_dkey,
                            data_template[key],
                        )
                continue

            regexp = re.compile("/data(\\d+)/")
            if bool(regexp.search(key)):
                data_index = regexp.search(key).group(1)
                if data_index not in data_entries:
                    data_entries[data_index] = get_data_object(measurement_type)
                clean_dkey = clean_key.split(f"data{data_index}/")[1]
                if hasattr(data_entries[data_index], clean_dkey):
                    if f"{key}/@units" in data_template:
                        setattr(
                            data_entries[data_index],
                            clean_dkey,
                            pd.to_numeric(
                                data_template[key], errors="coerce"
                            )  # data_template[key].astype(np.float64)
                            * ureg(data_template[f"{key}/@units"]),
                        )
                    else:
                        setattr(
                            data_entries[data_index],
                            clean_dkey,
                            data_template[key],
                        )
                continue

            clean_key, unit = split_value_unit(key.split(f"{measurement_type}/")[1])
            clean_key = rename_key(clean_key)
            if hasattr(eln_measurement, clean_key):
                if f"{key}/@units" in data_template:
                    setattr(
                        eln_measurement,
                        clean_key,
                        data_template[key] * ureg(data_template[f"{key}/@units"]),
                    )
                elif unit is not None:
                    if data_template[key] == "ERROR":
                        continue
                    if not pd.isna(data_template[key]):
                        continue
                    setattr(
                        eln_measurement,
                        clean_key,
                        data_template[key] * ureg(unit),
                    )
                else:
                    setattr(eln_measurement, clean_key, data_template[key])

        eln_measurement.results = []
        for data_entry in data_entries.values():
            eln_measurement.results.append(data_entry)

        for data_entry in contact_sets.values():
            eln_measurement.results.append(calc_best_fit_values(data_entry))

        if measurement_type == "Variable Temperature Measurement":
            eln_measurement.name = f"{measurement_type}: range {eln_measurement.starting_temperature} -> {eln_measurement.ending_temperature}"
        elif measurement_type == "Variable Field Measurement":
            eln_measurement.name = f"{measurement_type}: range {eln_measurement.minimum_field} -> {eln_measurement.maximum_field}"
        else:
            eln_measurement.name = f"{measurement_type}"

        yield eln_measurement


def instantiate_keithley(system, field_key, value, logger):
    """
    Create an instance of a Keithley component class.

    The class is choosen among the available ones in instrument module,
    based on which value is found in the `measurement_state_machine` section.
    """

    subsection_key = field_key.replace("_", "")
    subsection_value = value.replace(" ", "")
    for attr_name, attr_class in vars(hall_instrument).items():
        if to_snake_case(subsection_value) in to_snake_case(attr_name):
            logger.info(f"The {field_key} is {value}")
            if not hasattr(system, subsection_key):
                logger.warn(f"{subsection_key} subsection not found")
            setattr(system, subsection_key, attr_class())
            return to_snake_case(value)


def get_instrument(data_template: dict, logger):
    """
    Returns a hall instrument MSection representation form its corresponding
    nexus data_template.

    Args:
        data_template (dict): The nomad-parser-nexus data template.

    Yields:
        an Instrument object according to the schema in instrument.py
    """

    keithley_devices = [
        "electro_meter",
        "volt_meter",
        "current_meter",
        "current_source",
    ]
    keithley_components = {}
    other_components = [
        "system_parameters",
        "temperature_controller",
        "field_controller",
    ]
    instrument = hall_instrument.Instrument()
    instrument.temperature_controller = hall_instrument.TemperatureController()
    instrument.field_controller = hall_instrument.FieldController()
    temperature_domains: dict = {}
    for key in data_template:
        clean_key = to_snake_case(key)
        field_key = clean_key.split("/")[-1]
        value = data_template[key]
        if "/measurement_state_machine/" in clean_key:
            if hasattr(instrument, field_key):
                setattr(instrument, field_key, value)
                for k_device in keithley_devices:
                    if k_device == clean_key.split("/measurement_state_machine/")[1]:
                        keithley_components[k_device.replace("_", "")] = (
                            instantiate_keithley(instrument, field_key, value, logger)
                        )
        regexp = re.compile("temperature_domain_(\\d+)/")
        if bool(regexp.search(clean_key)):
            data_index = regexp.search(clean_key).group(1)
            if data_index not in temperature_domains:
                temperature_domains[data_index] = hall_instrument.TemperatureDomain()
            if hasattr(temperature_domains[data_index], field_key):
                setattr(temperature_domains[data_index], field_key, value)
            continue
        for instrument_comp in other_components:
            if instrument_comp is not None and f"/{instrument_comp}/" in clean_key:
                if hasattr(instrument, field_key):
                    setattr(instrument, field_key, value)
                elif hasattr(instrument, instrument_comp):
                    if hasattr(getattr(instrument, instrument_comp), field_key):
                        setattr(getattr(instrument, instrument_comp), field_key, value)
        for instrument_comp in list(keithley_components.keys()):
            if (
                instrument_comp is not None
                and f"/{keithley_components[instrument_comp]}/" in clean_key
            ):
                if hasattr(instrument, instrument_comp) and hasattr(
                    getattr(instrument, instrument_comp), field_key
                ):
                    setattr(getattr(instrument, instrument_comp), field_key, value)
    for t_domain in temperature_domains.values():
        instrument.m_add_sub_section(
            hall_instrument.Instrument.temperature_domain, t_domain
        )
    return instrument
