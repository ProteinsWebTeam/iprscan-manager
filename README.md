# Nextflow InterProScan Production Manager (NF-IPM)

A Nextflow pipeline to coordinate the calculation, import and cleaning of InterProScan matches for the InterPro database.

## Getting started

### Requirements:

* Nextflow >= 25.04.6

### Installation:

```bash
git clone https://github.com/ProteinsWebTeam/iprscan-manager.git
```

## Configuration

The IPM pipeline relies on two configuration file. Templates can be found in the `conf` directory.

### General configuration

The `conf/ipm.conf` is used for the general configuration of IPM and InterProScan.

> [!WARNING]  
> The InterProScan6 work directory will be extremely large! Make sure to point
> the `interproscan.workdir` field to a suitable location.

* **databases** - _configure database connections_
    * **intprscan**: _configuration for the postgre-sql interproscan database_
        * uri (`//Host:Port/Service`)
        * user
        * password
        * engine: `postgresql`
    * **uniparc**: _configuration for the `UATST` uniparc database (the read-only database)_
        * uri (`@Host:Port/Service`)
        * user
        * password
        * engine: `oracle`
* **interproscan** - _configure how InterProScan6 is run_
    * **device** - _specify the configuration for cpu or gpu execution_
        * **executable**: `"ebi-pf-team/interproscan6"` or path to a local InterProScan6 installation `main.nf` file
        * **executor**: Run InterProScan6 locally (`local`) or on SLURM (`slurm`)
        * **container**: Container runtime to use (e.g. `'docker'` or `'baremetal'` when the latter is supported)
        * **profile**: Specify any other InterProScan profiles to be used, e.g. `bulk`
        * **workdir**: Path to build the workdir. This directory can become extremely large!
        * **maxWorkers**: Set the `--maxWorkers` option in `interproscan6`
        * **config**: [Optional] Path to an Iprscan 6 config file. This needs to be used when running liscened software, otherwise iprscan will not know where to find the SignalP, Phobius and DeepTMHMM databases

### Application resource configuration

The resources allocated to instance of InterProScan is dependent on the application (or member database) that is running. These values are defined in the `conf/applications.conf` file.

The applications configuration is included by default in IPM so you only need to the `appsConfig` configuration in your configuration file if you are deferring from the values in this file.

* **appsConfig**
    **applications** _All applications default to the `light` configuration. To specify a different resource configuration list the application here_
        * `<application-name> = "<label>"`
    * **resources** _Define the resources for each label_
        * `<label> { memory = "X.GB", time = "Y.h" }`

## Usage

There are two required arguments:

Two arguments are required:
1. `-c` - Path to the `imp.conf` file
2. `--methods` - The name of the subworkflows (case-insensitive) as a comma separated list to run:
    * import
    * analyse
    * clean

To see all optional arguments use the `--help` option. Note some optional arguments are subworkflow specific.

### Import

The `IMPORT` subworkflow coordinates retrieving all (or the latest) protein sequences from the `UniParc` database 
and importing them into the InterProScan (e.g. `ISPRO`) database.

There are 2 optional arguments:
1. `--top-up` - Import new sequences only
2. `--max-upi` - Maximum sequence UPI to import

For example:
```bash
nextflow run main.nf -c conf/imp.conf --methods import --top-up
```

### Analyse

The `ANALYSE` subworkflow coordinates running InterProScan for every "active" analysis in the `ISPRO.ANALYSIS` table,
and persists all results in the `ISPRO` database.

There is one optional argument:
1. `--batch-size` - The maximum number of sequences to be analysed by each instance of InterProScan 6

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
