
## Installation
1. Clone this repository
2. Make sure you are using python 3.5 or later.
3. `python3 setup.py install`  optionally use `--user` for a user-specific installation


The script is  `trec_car_y3_conversion/y3_convert_ranking_to_ordering.py`


## Usage


```python3 trec_car_y3_conversion/y3_convert_ranking_to_ordering.py [-h] --outline-cbor OUTLINE_CBOR
                                         --output-directory OUTPUT_DIRECTORY
                                         [--run-directory RUN_DIRECTORY]
                                         [--run-file RUN_FILE]
                                         [--run-name RUN_NAME]
                                         [--include-text-from-paragraph-cbor INCLUDE_TEXT_FROM_PARAGRAPH_CBOR]
                                         [-k INT]

optional arguments:
  -h, --help            show this help message and exit
  --outline-cbor OUTLINE_CBOR
                        Path to an outline.cbor file
  --output-directory OUTPUT_DIRECTORY
                        Output directory (writes on json file per run)
  --run-directory RUN_DIRECTORY
                        Path to a directory containing all runfiles to be
                        parsed (uses run name given in trec run files).
  --run-file RUN_FILE   Single runfiles to be parsed.
  --run-name RUN_NAME   overwrite run name in run-file with this one.
  --include-text-from-paragraph-cbor INCLUDE_TEXT_FROM_PARAGRAPH_CBOR
                        If set, loads paragraph text from the paragraph corpus
                        .cbor file.
  -k INT                Maximum number of paragraphs to pull from each query
                        in a runfile. (Default is 10)
```


After running the script, the output-directory will contain multiple *.jsonl files,
each named according to the `run-name` given in the trec run file or overwritten by the command line argument.

