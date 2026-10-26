# tap-easyecom

`tap-easyecom` is a Singer tap for EasyEcom.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install tap-easyecom
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-easyecom.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-easyecom --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-easyecom --about
```

### Receipt replication

`receipts` requests new and modified GRNs from `/Grn/V2/getGrnDetails` using
`lastupdate_after` and stores `grn_updated_at` as its replication key.
Completed GRNs remain eligible when edited; `grn_created_at` remains in the
schema but no longer drives extraction. The `open_receipts` stream and its
unfinished-GRN queue have been removed.

The first run uses `start_date`. Subsequent runs request a 12-hour overlap before
the stored modification bookmark without subtracting from the bookmark itself.
Consumers must continue upserting GRNs by `grn_id` and deduplicating receipt
details by their EasyEcom IDs.

#### Existing-job cutover

1. Stop the old job before switching its tap version.
2. Regenerate the Singer catalog with `tap-easyecom --config config.json --discover`.
   Retain the intended selections, select `receipts`, and remove any
   `open_receipts` selection or downstream stream mapping. Creation-based receipt
   catalogs are rejected rather than silently restoring the old cursor.
3. Back up the job state. Clear only `bookmarks.receipts` and remove the obsolete
   `bookmarks.open_receipts` entry; preserve every other stream's state.
   Do not relabel a `grn_created_at` bookmark as `grn_updated_at`. If an old
   creation bookmark is supplied, the SDK ignores it and starts from `start_date`.
4. Set `start_date` to cover the required historical modification window and run
   a deliberate replay. A recent date will not recover earlier missed edits.
   Verify receipt upserts and the emitted `grn_updated_at` bookmark before
   resuming the schedule.

The source must populate `grn_updated_at` for new GRNs and advance it for
relevant edits, including receipt-item changes and edits to completed GRNs.
The 12-hour overlap only covers source visibility delays within that window.
Validate these provider guarantees and modification-filter pagination on the
intended tenant before cutover; local tests use synthetic HTTP responses.

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-easyecom` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-easyecom --version
tap-easyecom --help
tap-easyecom --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-easyecom` CLI interface directly using `poetry run`:

```bash
poetry run tap-easyecom --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-easyecom
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-easyecom --version
# OR run a test `elt` pipeline:
meltano run tap-easyecom target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
