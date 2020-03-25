# tap-covid-19

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls CSV files from [GitHub v3 API ](https://developer.github.com/v3/).
- Extracts the following resources:
  - CSV Data Files: [Git API Search](https://developer.github.com/v3/search/#search-code) with [filename and extension filters](https://help.github.com/en/articles/searching-code) from the following COVID-19 Repositories; streaming in new/changed files:
    - [Johns Hopkins CSSE Data](https://github.com/CSSEGISandData/COVID-19)
      - [jh_csse_daily](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports)
      - [jh_csse_time_series](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series)
    - [EU Data](https://github.com/covid19-eu-zh/covid19-eu-data)
      - [eu_daily](https://github.com/covid19-eu-zh/covid19-eu-data/tree/master/dataset)
    - [Italy Data](https://github.com/pcm-dpc/COVID-19)
      - [ita_national](https://github.com/pcm-dpc/COVID-19/tree/master/dati-andamento-nazionale)
      - [ita_provinces](https://github.com/pcm-dpc/COVID-19/tree/master/dati-province)
      - [ita_regions](https://github.com/pcm-dpc/COVID-19/tree/master/dati-regioni)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state (file last-modified in GitHub)


## Streams

[jh_csse_daily_files](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports)
- Repository: CSSEGISandData/COVID-19
- Folder: csse_covid_19_data/csse_covid_19_daily_reports
- Search Endpoint: https://api.github.com/search/code?q=path:csse_covid_19_data/csse_covid_19_daily_reports+extension:csv+repo:CSSEGISandData/COVID-19
- File Endpoint: https://api.github.com/repos/CSSEGISandData/COVID-19/contents/[GIT_FILE_PATH]
- Primary key fields: path
- Replication strategy: INCREMENTAL (Search ALL, filter results)
  - Bookmark field: last_modified
- Transformations: Remove _links node, remove content node, add repository fielda

[jh_csse_daily](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports)
- Primary key fields: date, row_number
- Replication strategy: FULL_TABLE (ALL for each model_file)
- Transformations: Decode, parse jh_daily_file content, cleanse location fields, and convert to JSON


## Authentication
This tap requires a GitHub API Token. See Step 3 below.
Even though this tap pulls from public GitHub repositories, API request limits are much lower without a token.

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-covid-19
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. This tap connects to GitHub with a [GitHub OAuth2 Token](https://developer.github.com/v3/#authentication). This may be a [Personal Access Token](https://github.com/settings/tokens) or [Create an authorization for an App](https://developer.github.com/v3/oauth_authorizations/#create-a-new-authorization). 

    ```json
    {
        "api_token": "YOUR_GITHUB_API_TOKEN",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-covid-19 <api_user_email@your_company.com>"
    }
    ```

    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "jh_csse_daily",
        "bookmarks": {
            "jh_csse_daily_files": "2019-10-13T19:53:36.000000Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-covid-19 --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-covid-19 --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-covid-19 --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-covid-19 --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the COVID-19 tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_covid_19 -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.44/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-covid-19 --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 14826 messages for 2 streams.

        2 schema messages
    14821 record messages
        3 state messages

    Details by stream:
    +---------------------+---------+---------+
    | stream              | records | schemas |
    +---------------------+---------+---------+
    | jh_csse_daily       | 14758   | 1       |
    | jh_csse_daily_files | 63      | 1       |
    +---------------------+---------+---------+


    ```
---

Copyright &copy; 2020 Stitch
