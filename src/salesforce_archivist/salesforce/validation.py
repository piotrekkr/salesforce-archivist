import csv
import os


class ValidatedContentVersion:
    def __init__(self, path: str, checksum: str):
        self._path = path
        self._checksum = checksum

    @property
    def path(self) -> str:
        return self._path

    @property
    def checksum(self) -> str:
        return self._checksum


class ValidatedContentVersionList:
    def __init__(self, data_dir: str):
        self._data: dict[str, ValidatedContentVersion] = {}
        self._path = os.path.join(data_dir, "validated_versions.csv")
        if os.path.exists(self._path):
            self._load_data()

    def _load_data(self) -> None:
        with open(self._path) as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                version = ValidatedContentVersion(checksum=row[0], path=row[1])
                self.add_version(version)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w") as file:
            writer = csv.writer(file)
            writer.writerow(["Checksum", "Path"])
            for version_id, version in self._data.items():
                writer.writerow([
                    version.checksum,
                    version.path,
                ])

    def add_version(self, version: ValidatedContentVersion) -> None:
        self._data[version.path] = version

    def is_validated(self, path: str) -> bool:
        return path in self._data

    def get_version(self, path: str) -> ValidatedContentVersion | None:
        return self._data.get(path)
