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
> the `interproscan.runtime.workdir` field to a suitable location.

* **databases** - _configure database connections_
    * **iprscanIprscan**: uri (`@Host:Port/Service`), username and password for the `iprscan` user in the InterProScan [ISPRO] database
    * **iprscanUniParc**: uri (`@Host:Port/Service`), username and password for the `uniparc` user in the InterProScan [ISPRO] database
    * **uniprot**: uri (`@Host:Port/Service`), username and password for the UniParc read-only database
* **interproscan** - _configure how InterProScan6 is run_
    * **runtime**
        * **executable**: `"ebi-pf-team/interproscan6"` or path to a local InterProScan6 installation `main.nf` file
        * **executor**: Run InterProScan6 locally (`local`) or on SLURM (`slurm`)
        * **container**: Container runtime to use (e.g. `'docker'` or `'baremetal'` when the latter is supported)
        * **workdir**: Path to build the workdir. This directory can become extremely large!
    * **sbatch**
        * **enabled**: Boolean. Submit InterProScan6 as a new job to the cluster, else InterProScan6 will run within the IPM cluster job
        * **nodes**: Nodes to be assigned to the InterProScan6 cluster job
        * **cpus**: CPUs to be assigned to the InterProScan6 cluster job
        * **memory**: Memory to be assigned to the InterProScan6 cluster job
        * **time**: Time to be assigned to the InterProScan6 cluster job
        * **jobName**: Name to be assigned to the InterProScan6 cluster job
        * **jobLog**: Name of the log file for the InterProScan6 cluster job - leave as `null` or `""` to not write a log file
        * **jobErr**: Name of the error file for the InterProScan6 cluster job - leave as `null` or `""` to not write an error file

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

There are no optional arguments.

For example:
```bash
nextflow run main.nf -c conf/imp.conf --methods analyse
```

### Clean

The `CLEAN` subworkflow deletes obsolete data for analyses listed as active `'Y'` in the IprScan database.

There is one optional argument:
1. `--analyses` - IDs od analyses to clean (default: all)

For example:
```bash
nextflow run main.nf -c conf/imp.conf --methods clean
```
