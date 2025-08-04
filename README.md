> [!CAUTION]
> The InterProScan Production Manager is currently under active development and is not yet stable enough for a full release.

# InterProScan Production Manager (IPM)

A Nextflow pipeline to coordinate the calculation, import and cleaning of InterProScan matches for the InterPro database.

## Getting started

### Requirements:

* Nextflow >= 24.10.4

### Installation:

```bash
git clone https://github.com/ProteinsWebTeam/iprscan-manager.git
```

## Configuration

The IPM pipeline relies on one configuration file. A template can be found in
`./conf/ipm.conf`.

> [!WARNING]  
> The InterProScan6 work directory will be extremely large! Make sure to point
> the `interproscan.workdir` field to a suitable location.

* **databases** - _configure database connections_
    * **intprscan**: _configuration for the interproscan database_
        * **intprscan**: _configuration for the intprscan schema_
            * uri (`//Host:Port/Service`)
            * user
            * password
            * engine: `postgresql`
        * **uniparc**: _configuration for the uniparc schema - this connects to the current ISPRO/DEV/TST oracle db_
            * uri (`@Host:Port/Service`)
            * user
            * password
            * engine: `oracle`
    * **uniparc**: _configuration for the `UATST` uniparc database (the read-only database)_
        * uri (`@Host:Port/Service`)
        * user
        * password
        * engine: `oracle`
* **interproscan** - _configure how InterProScan6 is run_
    * **executable**: `"ebi-pf-team/interproscan6"` or path to a local InterProScan6 installation `main.nf` file
    * **executor**: Run InterProScan6 locally (`local`) or on SLURM (`slurm`)
    * **container**: Container runtime to use (e.g. `'docker'` or `'baremetal'` when the latter is supported)
    * **workdir**: Path to build the workdir. This directory can become extremely large!
    * **maxWorkers**: Set the `--maxWorkers` option in `interproscan6`
    * **config**: [Optional] Path to an Iprscan 6 config file. This needs to be used when running liscened software, else iprscan won't know where to find the SignalP, Phobius and DeepTMHMM databases

## Usage

There are two required arguments:

Two arguments are required:
1. `-c` - Path to the `imp.conf` file
2. `--methods` - The name of the subworkflows (case-insensitive) as a comma separated list to run

### Import

The `IMPORT` subworkflow coordinates import protein sequences from UniProt into the InterProScan database.

...

### Analyse

The `ANALYSE` subworkflow coordinates running InterProScan for every "active" analysis in the `ISPRO.ANALYSIS` table,
and persists all results in the `ISPRO` database.

There are no optional arguments.

For example:
```bash
nextflow run main.nf -c conf/imp.conf --methods analyse
```

### Clean

The `CLEAN` subworkflow deletes obsolete data for analyses listed as active `'Y'` in the IprScan database.

There is one optional argument:
1. `--analyses` - IDs of analyses to clean (default: all)

For example:
```bash
nextflow run main.nf -c conf/imp.conf --methods clean
```
