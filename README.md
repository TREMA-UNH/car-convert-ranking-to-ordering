
## Installation
1. Clone this repository
2. Make sure you are using python 3.5 or later.
3. `python3 setup.py install`  optionally use `--user` for a user-specific installation

The conversion script is  `rparsing/cbor_parsing.py`


## Usage

```
python3 rparsing/cbor_parsing.py [-n INT] outline_cbor run_directory paragraph_cbor
```

Where:

* **-n**: For each run, # of retrieve documents to take from each query (default is 10).
* **outline_cbor**: Path to outline cbor file (such as benchmarkY2.cbor-outlines.cbor)
* **run_directory**: Path to a directory containing all of the TREC CAR run files that you want to parse and create json files from. 
* **paragraph_cbor**: Path to a paragraph corpus .cbor file. Used to lookup a paragraph's text, entity links, etc. during run time.

Output:

Creates a jsons/ directory. Each .json in directory is named according to RUNID_PAGETITLE.json, where RUNID corresponds to the run's name and PAGETITLE corresponds to the title of the page that was query in this run (spaces replaced with underscores).
