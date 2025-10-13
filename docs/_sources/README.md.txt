**Versions Supported**

![Python](https://img.shields.io/badge/python-3.11-blue)

**Language Stats**

![Language count](https://img.shields.io/github/languages/count/thevickypedia/PyS3Uploader)
![Code coverage](https://img.shields.io/github/languages/top/thevickypedia/PyS3Uploader)

**Repo Stats**

[![GitHub](https://img.shields.io/github/license/thevickypedia/PyS3Uploader)][license]
[![GitHub repo size](https://img.shields.io/github/repo-size/thevickypedia/PyS3Uploader)][repo]
[![GitHub code size](https://img.shields.io/github/languages/code-size/thevickypedia/PyS3Uploader)][repo]

**Activity**

[![GitHub Repo created](https://img.shields.io/date/1760313686)][repo]
[![GitHub commit activity](https://img.shields.io/github/commit-activity/y/thevickypedia/PyS3Uploader)][repo]
[![GitHub last commit](https://img.shields.io/github/last-commit/thevickypedia/PyS3Uploader)][repo]

**Build Status**

[![pypi-publish][gha-pypi-badge]][gha-pypi]
[![pages-build-deployment][gha-pages-badge]][gha-pages]

# PyS3Uploader
Python module to upload an entire directory to an S3 bucket.

### Installation
```shell
pip install PyS3Uploader
```

### Usage

##### Upload objects in parallel
```python
import s3

if __name__ == '__main__':
    wrapper = s3.Uploader(
        bucket_name="BUCKET_NAME",
        upload_dir="FULL_PATH_TO_UPLOAD",
        exclude_prefix="PART_OF_UPLOAD_DIR_TO_EXCLUDE"
    )
    wrapper.run_in_parallel()
```

##### Upload objects in sequence
```python
import s3

if __name__ == '__main__':
    wrapper = s3.Uploader(
        bucket_name="BUCKET_NAME",
        upload_dir="FULL_PATH_TO_UPLOAD",
        exclude_prefix="PART_OF_UPLOAD_DIR_TO_EXCLUDE"
    )
    wrapper.run()
```

#### Mandatory arg
- **bucket_name** - Name of the s3 bucket.
- **upload_dir** - Directory to upload.

#### Optional kwargs
- **s3_prefix** - S3 object prefix for each file. Defaults to ``None``
- **exclude_prefix** - Path in ``upload_dir`` that has to be excluded in object keys. Defaults to `None`
- **skip_dot_files** - Boolean flag to skip dot files. Defaults to ``True``
- **overwrite** - Boolean flag to overwrite files present in S3. Defaults to ``False``
- **file_exclusion** - Sequence of files to exclude during upload. Defaults to ``None``
- **folder_exclusion** - Sequence of directories to exclude during upload. Defaults to ``None``
- **logger** - Bring your own custom pre-configured logger. Defaults to on-screen logging.
<br><br>
- **region_name** - AWS region name. Defaults to the env var `AWS_DEFAULT_REGION`
- **profile_name** - AWS profile name. Defaults to the env var `PROFILE_NAME`
- **aws_access_key_id** - AWS access key ID. Defaults to the env var `AWS_ACCESS_KEY_ID`
- **aws_secret_access_key** - AWS secret access key. Defaults to the env var `AWS_SECRET_ACCESS_KEY`
> AWS values are loaded from env vars or the default config at `~/.aws/config` / `~/.aws/credentials`

### Coding Standards
Docstring format: [`Google`](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) <br>
Styling conventions: [`PEP 8`](https://www.python.org/dev/peps/pep-0008/) <br>
Clean code with pre-commit hooks: [`flake8`](https://flake8.pycqa.org/en/latest/) and
[`isort`](https://pycqa.github.io/isort/)

## [Release Notes][release-notes]
**Requirement**
```shell
python -m pip install gitverse
```

**Usage**
```shell
gitverse-release reverse -f release_notes.rst -t 'Release Notes'
```

## Linting
`pre-commit` will ensure linting, run pytest, generate runbook & release notes, and validate hyperlinks in ALL
markdown files (including Wiki pages)

**Requirement**
```shell
pip install sphinx==5.1.1 pre-commit recommonmark
```

**Usage**
```shell
pre-commit run --all-files
```

## Pypi Package
[![pypi-module][label-pypi-package]][pypi-repo]

[https://pypi.org/project/PyS3Uploader/][pypi]

## Runbook
[![made-with-sphinx-doc][label-sphinx-doc]][sphinx]

[https://thevickypedia.github.io/PyS3Uploader/][runbook]

## License & copyright

&copy; Vignesh Rao

Licensed under the [MIT License][license]

[license]: https://github.com/thevickypedia/PyS3Uploader/blob/main/LICENSE
[release-notes]: https://github.com/thevickypedia/PyS3Uploader/blob/main/release_notes.rst
[pypi]: https://pypi.org/project/PyS3Uploader/
[pypi-tutorials]: https://packaging.python.org/tutorials/packaging-projects/
[pypi-logo]: https://img.shields.io/badge/Software%20Repository-pypi-1f425f.svg
[repo]: https://api.github.com/repos/thevickypedia/PyS3Uploader
[gha-pages-badge]: https://github.com/thevickypedia/PyS3Uploader/actions/workflows/pages/pages-build-deployment/badge.svg
[gha-pypi-badge]: https://github.com/thevickypedia/PyS3Uploader/actions/workflows/python-publish.yml/badge.svg
[gha-pages]: https://github.com/thevickypedia/PyS3Uploader/actions/workflows/pages/pages-build-deployment
[gha-pypi]: https://github.com/thevickypedia/PyS3Uploader/actions/workflows/python-publish.yml
[sphinx]: https://www.sphinx-doc.org/en/master/man/sphinx-autogen.html
[label-sphinx-doc]: https://img.shields.io/badge/Made%20with-Sphinx-blue?style=for-the-badge&logo=Sphinx
[runbook]: https://thevickypedia.github.io/PyS3Uploader/
[label-pypi-package]: https://img.shields.io/badge/Pypi%20Package-PyS3Uploader-blue?style=for-the-badge&logo=Python
[pypi-repo]: https://packaging.python.org/tutorials/packaging-projects/
