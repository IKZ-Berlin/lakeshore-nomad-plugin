import re
import json


def parse_file_to_dict(filepath):
    with open(filepath, "r", encoding="latin1") as file:
        content = file.read()

    sections_pattern = r"\[(.*?)\](.*?)(?=\n\[|\Z)"
    sections = re.findall(sections_pattern, content, re.DOTALL)

    data_dict = {}
    for section_name, section_content in sections:
        data_dict[section_name] = {}

        if section_name == "Measurements":
            steps_pattern = r"<Step\s*\d+:\s*(.*?)>(.*?)(?=\n<Step|\Z)"
            steps = re.findall(steps_pattern, section_content, re.DOTALL)

            for step_name, step_content in steps:
                step_dict = {}

                contact_sets_pattern = (
                    r"(Contact Sets:.*?)(?=\n\nContact Sets:|\n\n<|\Z)"
                )
                contact_sets_content = re.findall(
                    contact_sets_pattern, step_content, re.DOTALL
                )
                params_lists = []
                for contact_set_content in contact_sets_content:
                    lines = contact_set_content.strip().split("\n")
                    contact_set = {"Name": re.split(r"[=|:]", lines[0])[1].strip()}
                    for index, line in enumerate(lines[1:]):

                        parts = re.split(r"[=|:]", line, maxsplit=1)
                        if len(parts) == 2:
                            key, value = parts
                            value = value.strip()
                            if "[" in key and "]" in key:
                                base_key, unit = key.split("[", 1)
                                base_key = base_key.strip()
                                unit = unit.split("]", 1)[0].strip()
                                contact_set[base_key] = value
                                contact_set[f"{base_key}_unit"] = unit
                            elif "[" in value and "]" in value:
                                contact_set[key.strip()] = value.split("[")[0].strip()
                                contact_set[f"{key.strip()}_unit"] = (
                                    value.split("[")[1].split("]")[0].strip()
                                )
                            else:
                                contact_set[key.strip()] = value
                        else:
                            data_rows = [row.split("\t") for row in lines[index + 1 :]]
                            for parameter in data_rows[0]:
                                contact_set[parameter.split("[")[0].strip()] = [
                                    value[0] for value in data_rows[1:] if value
                                ]
                                contact_set[
                                    f"{parameter.split('[')[0].strip()}_unit"
                                ] = (parameter.split("[")[1].split("]")[0].strip())
                            break
                    params_lists.append(contact_set)

                current_chunk = []
                for line in step_content.split("\n"):
                    line = line.strip()
                    if line:
                        current_chunk.append(line)
                    if not line:
                        if current_chunk:
                            if "Contact Sets" in current_chunk[0]:
                                step_dict["Contact Sets"] = params_lists
                                break
                            for index, line in enumerate(current_chunk):
                                parts = re.split(r"[=|:]", line, maxsplit=1)
                                if len(parts) == 2:
                                    key, value = parts
                                    value = value.strip()
                                    if "[" in key and "]" in key:
                                        base_key, unit = key.split("[", 1)
                                        base_key = base_key.strip()
                                        unit = unit.split("]", 1)[0].strip()
                                        step_dict[base_key] = value
                                        step_dict[f"{base_key}_unit"] = unit
                                    elif "[" in value and "]" in value:
                                        step_dict[key.strip()] = value.split("[")[
                                            0
                                        ].strip()
                                        step_dict[f"{key.strip()}_unit"] = (
                                            value.split("[")[1].split("]")[0].strip()
                                        )
                                    else:
                                        step_dict[key.strip()] = value
                                else:
                                    data_rows = [
                                        row.split("\t") for row in current_chunk
                                    ]
                                    for parameter in data_rows[0]:
                                        step_dict[parameter.split("[")[0].strip()] = [
                                            value[0] for value in data_rows[1:] if value
                                        ]
                                        step_dict[
                                            f"{parameter.split('[')[0].strip()}_unit"
                                        ] = (
                                            parameter.split("[")[1]
                                            .split("]")[0]
                                            .strip()
                                        )
                                    break
                            current_chunk = []

                data_dict[section_name][f"{step_name}"] = step_dict
        else:
            key_value_pairs = re.findall(
                r"^(.*?):\s*(.*?)$", section_content, re.MULTILINE
            )
            for key, value in key_value_pairs:
                data_dict[section_name][key] = value

            subsections_pattern = r"<(.*?)>(.*?)(?=\n<|\Z)"
            subsections = re.findall(subsections_pattern, section_content, re.DOTALL)
            for subsection_name, subsection_content in subsections:
                if subsection_name not in data_dict[section_name]:
                    data_dict[section_name][subsection_name] = {}

    return data_dict
