## CrocoLake-Matlab

CrocoLakeTools is a collection of Matlab scripts that shows how to interface with CrocoLake and Argo's parquet databases.

### Table of Contents
1. [Usage](#usage)
2. [Databases](#databases)
3. [Contact](#contact)

### Usage
To run the scripts, simply run them from your Matlab workspace. Note that you might need Matlab R2022 or newer for compatibility with the parquet format.

Each scripts loads, filters, and visualizes a different database (Argo or CrocoLake, physical or biogeochemical observations). Before running the script, make sure you downloaded the database: each script contains a commented line of code of the type `download_database(db_name, db_type, db_qc)` just before the first instruction; you can uncomment this the first time you run the script (or copy-paste and run in the command window). For example, in `Example_ARGO_PHY_QC.m` it is `% download_database("ARGO","PHY",true)`.

### Databases

The following databases are currently available:

* Argo 'ALL': contains all real time and adjusted variables as reported in the core ('<PLATFORM_NUMBER>_prof.nc') and synthetic ('<PLATFORM_NUMBER>_Sprof.nc') profile files, for the physical and biogeochemical versions respectively;
* Argo 'QC': contains the best available data, that is real time values are reported only when delayed values are not available. More details here.
* CrocoLake: contains the best available data from Argo, GLODAP, and Spray Gliders. More details here.

Each database comes in 'PHY' and 'BGC' versions.

### Contact

For any questions, bugs, missing information, etc, open an issue or [get in touch](enrico.milanese@whoi.edu)!
