# mapper-leie

## Overview

The U.S. Department of Health and Human Services, Office of Inspector General's List of Excluded Individuals/Entities (LEIE) provides information to the health care industry, 
patients and the public regarding individuals and entities currently excluded from participation in Medicare, Medicaid, and all other Federal health care programs.

The [leie-mapper.py](leie_mapper.py) python script converts this list into a json file ready to load into Senzing.  This list is available to the public and can be 
downloaded [here](https://oig.hhs.gov/exclusions/exclusions_list.asp).  

The list of exclusion types can be found [here](https://oig.hhs.gov/exclusions/authorities.asp) and their FAQ can be found [here](https://oig.hhs.gov/faqs/exclusions-faq.asp).

Loading this data into Senzing requires additional features and configurations. These are contained in the
[leie_config_updates.g2c](leie_config_updates.g2c) file.

Usage:

```console
python3 leie-mapper.py --help
usage: leie-mapper.py [-h] [-i INPUT_FILE] [-o OUTPUT_FILE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE, --input_file INPUT_FILE
                        the name of the input file
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        the name of the output file
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
```

## Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuring Senzing](#configuring-senzing)
4. [Running the mapper](#running-the-mapper)
5. [Loading into Senzing](#loading-into-senzing)
6. [Mapping other data sources](#mapping-other-data-sources)

### Prerequisites

- Python 3.6 or higher

### Installation

Place the following files on a directory of your choice:

- [leie_mapper.py](leie_mapper.py)
- [leie_config_updates.g2c](leie_config_updates.g2c)

### Configuring Senzing

*Note:* This only needs to be performed one time! In fact you may want to add these configuration updates to a master configuration file for all your data sources.

From your Senzing project directory:

```console
python3 G2ConfigTool.py <path-to-file>/leie_config_updates.g2c
```

This will step you through the process of adding the data sources, features, attributes and any other settings needed to load this watch list data into 
Senzing. After each command you will see a status message saying "success" or "already exists".  For instance, if you run the script twice, the second time through they will all 
say "already exists" which is OK.

### Running the mapper

Download the raw file from: [here](https://oig.hhs.gov/exclusions/exclusions_list.asp)

There you can download the complete and updated list as well as monthly additions and reinstatements.  All 3 files have the same structure.  The full file is small enough that you
can map and load the whole file every time and Senzing will not duplicate records.  But you do have the option of just loading the full file once and only applying the monthly 
updates thereafter.

Then run the mapper.  Example usage:

```console
python3 leie_mapper.py -i /path/to/leie-updated-yyyy-mm-dd.csv -o /path/to/leie-full-yyyy-mm-dd.json
```

### Loading into Senzing

If you use the G2Loader program to load your data, from your project directory ...

```console
python3 G2Loader.py -f /path/to/leie-full-yyyy-mm-dd.json
```

This data set currently only contains about 75k records and loads in just a few minutes on decent hardware.

### Mapping other data sources

While not required, look for the following identifiers in your other data sets:
- NPI_NUMBER - The NPI (National Provider Identifier) has replaced the UPIN (see question below) as the unique number used to identify health care providers. The Centers for 
Medicaid & Medicare Services first began assigning NPIs in 2006, and providers were required to use NPIs as of mid-2008.
- UPIN_NUMBER - The UPIN (Unique Physician Identification Number) was established by the Centers for Medicare & Medicaid Services as a unique provider identifier in lieu of the
SSN. UPINs were assigned to physicians as well as certain non-physician practitioners and medical group practices. CMS no longer maintains the UPIN registry.

*Note: The Downloadable Database does not contain SSNs or EINs. Therefore, verification of specific individuals or entities through the use of the SSN or EIN must be done via the 
Online Searchable Database.*
