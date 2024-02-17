# Salesforce Archivist

This project aims to ease the process of archiving files uploaded to Salesforce. Storage in Salesforce can be
very expensive and often there is no real need to keep old files inside Salesforce. Using this project allows
you to download files attached to objects in Salesforce and store them on disk. To be sure that download process
was executed correctly there is also a way to validate all downloaded files on disk against checksums from Salesforce.

## Motivation

I was recently tasked with cleaning old files and archiving those in GCS bucket. I did not work with Salesforce
before, so I started to search for existing tools. I found a few but only
[hardisgroupcom/sfdx-hardis](https://github.com/hardisgroupcom/sfdx-hardis) was free and could be used in CLI
inside a VM. `sfdx-hardis` has `hardis:org:files:export` command that is responsible for file export. It is a good tool
but has few shortcomings that can appear quite fast after using it with existing SF org:
- **It is focused on object** - This approach is problematic when one object has thousands of files
  attached to it and something breaks or some files were not downloaded properly. Re-run skips download if object
  directory exist on disk. You need to remove whole directory to trigger download again.
- **Downloading big files sometimes breaks** - I encountered some issues when downloading big files (>1G) and was forced
  to manually download some files using `curl`.
- **No download validation** - There seems to be no way to validate if all files were downloaded and if checksums match
- **It does not calculate API calls correctly** - Before actual download starts you are presented with statistics about
  how many calls will be issued. The problem is that those calculations do not take into account actual download calls.
  You can hit API limit pretty quickly and lock all other apps using same REST API.

## Features

- **Focused on files** - Based on configuration it fetches a list of files to download and process it. In case of error
  re-run only downloads missing files. No problems when SF object has thousands of files attached.
- **Reuse already downloaded files** - Because same files can be linked to multiple objects, it will reuse already
  downloaded file instead of downloading it again for new objects
- **Big files download** - using python `simple-salesforce` library can handle downloading big files pretty well.
- **Validation command** - When download is complete you can run validation command to check disk files against
  checksums from Salesforce. No additional API calls made during validation process.
- **Mindful of Salesforce API limits** - Will pause download when configured API usage is hit and resume automatically
  when usage drops.
- **Parallel downloads and validation** - Can download and validate in parallel using threads.

## Tech stack

Project is implemented in `Python 3.11` and is using `poetry` as package manager. Main libraries used:
- [`simple-salesforce`](https://github.com/simple-salesforce/simple-salesforce) - handling Salesforce API
- [`click`](https://github.com/pallets/click) - working with CLI
- [`PyYaml`](https://github.com/yaml/pyyaml/) - config parsing
- [`schema`](https://github.com/keleshev/schema) - config validation
- [`pytest`](https://github.com/pytest-dev/pytest) - testing
- [`mypy`](https://github.com/python/mypy) - static type checks
- [`ruff`](https://github.com/astral-sh/ruff) - linting and code style
- [`poethepoet`](https://github.com/nat-n/poethepoet) - task automation

There are also some other smaller libraries used. You can check them inside `pyproject.toml`.

## Installation

### Plain Python

1. [Install `python`](https://www.python.org/downloads/) (version `3.11` or greater)
2. [Install `poetry`](https://python-poetry.org/docs/#installation)
3. Clone project
   ```shell
   git clone git@github.com:piotrekkr/salesforce-archivist.git
   cd salesforce-archivist
   ```
4. Install packages
   ```shell
   poetry install
   ```

### Docker

//TODO

### Devcontainer

// TODO

## Running project

### Authenticate in Salesforce

Currently, this project can work with JWT authorization flow. You can follow
[this tutorial](https://developer.salesforce.com/docs/atlas.en-us.sfdx_dev.meta/sfdx_dev/sfdx_dev_auth_jwt_flow.htm)
to configure private key, self-signed certificate and a connected app.
Following first and second step should be enough to make it work.

### Configuration

Example configuration file contains comments explaining purpose of each configuration option. You can copy it and
adjust to your own needs.

```shell
cp config.example.yaml config.yaml
```

> ℹ️ Before you can use private key (server.key) in `config.yaml` you should encode it as `base64` string.

### Running commands

First activate `poetry` shell
```shell
poetry shell
```

When in poetry shell you should be able to use `archivist` command like

```shell
# download
archivist download

# validation
archivist validate
```

## Design

Relation between Salesforce object (entities) and files looks like this:

```mermaid
erDiagram
    ContentDocumentLink {
        reference ContentDocumentId
        reference LinkedEntityId
    }
    ContentDocument {
        string Id
        datetime ContentModifiedDate
    }
    "Entity (User, Event,...)" {
        string Id

    }
    ContentVersion {
        string Id
        reference ContentDocumentId
        string Checksum
        string Title
        string FileExtension
    }
    ContentDocument ||--|{ ContentVersion : ""
    ContentDocumentLink }o--|| ContentDocument : ""
    ContentDocumentLink }o--|| "Entity (User, Event,...)" : ""
```
Files (`ContentDocument` objects) can be linked to multiple entities (SF objects like `User`, `Case`, and so on).
File can have multiple versions (`ContentVersion` objects).

### Download

Based on configuration, download process will work as follows:
1. If exists, load already downloaded files list (`{data_dir}/downloaded_versions.csv)`).
2. For each object type defined in configuration:
   1. Load existing content document link list (`{data_dir}/{obj_type}/document_links.csv`) or download from
      Salesforce with specified conditions.
   2. Based on document link list, load content version list (`{data_dir}/{obj_type}/content_versions.csv`) or
      download it from Salesforce.
   3. Based on those two lists generate in memory mapping of files to download with objects they are linked to.
   4. For each file on list above:
      1. Combine file path (`{data_dir}/{obj_type}/files/{obj_id|custom_field}/{doc_id}_{version_num}_{id}_{title}.{ext}`)
      2. Check if file is already on disk or was downloaded for some other object, and if needed, copy file to new
         location and update downloaded files list.
      3. If above is not the case then fetch file from Salesforce and update downloaded files list.
      4. Check API limits, and if needed, wait for usage to drop below threshold.
   5. Save downloaded files list on disk.
3. When all object download is complete, show statistics.

### Validation

Based on configuration, validation process will work as follows:
1. If exists, load already validated files list (`{data_dir}/validated_versions.csv)`).
2. For each object type defined in configuration:
   1. Load existing content document link list (`{data_dir}/{obj_type}/document_links.csv`) or download from
      Salesforce with specified conditions.
   2. Based on document link list, load content version list (`{data_dir}/{obj_type}/content_versions.csv`) or
      download it from Salesforce.
   3. Based on those two lists generate in memory mapping of files to download with objects they are linked to.
   4. For each file on list above:
      1. If file does not exist on disk, or was already validated and checksum does not match with Salesforce, then mark
         file as invalid.
      2. If file was not validated before, calculate checksum of disk file, update validated list, compare checksum and
         if needed mark file as invalid.
   5. Save validated files list on disk.
3. When validation is complete, show statistics.

## HOWTOs

### How to re-download content version list and document link list?

You can remove CSV files from disk and nex download will download full lists again.
```shell
# for chosen type
rm -rf {data_dir}/{object_type}/*.csv

# or for all types
rm -rf {data_dir}/*/*.csv
```

### How to force revalidate all files again?

Already calculated checksums for downloaded files are kept in `{data_dir}/validated_versions.csv`.
You can remove this file or selected lines from inside this file. This will trigger full validation again.

## Contributing

// TODO

## TODO

- Use pydantic instead of schema
- Add docker, compose and devcontainer support
- Add some example terraform and/or ansible to use for deploy to VM in cloud
- Add options to force re-download versions and document link lists
- Add global config options to set date ranges if none was specified on object type level
