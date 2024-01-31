import csv
import tempfile


def gen_csv(data: list[list[str]], path: str):
    with open(path, newline="", mode="w") as csv_file:
        writer = csv.writer(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in data:
            writer.writerow(row)


def gen_temp_csv_files(data: list[list[list[str]]], dir_name: str):
    for file_data in data:
        temp_file_path = tempfile.mkstemp(suffix=".csv", dir=dir_name, text=True)[1]
        gen_csv(data=file_data, path=temp_file_path)
