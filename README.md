
## Installation
1. Clone this repository
2. Make sure you are using python 3.5 or later.
3. `python3 setup.py install`  optionally use `--user` for a user-specific installation


This suite provides the following executable scripts

- `y3_convert_ranking_to_ordering.py` for converting passage rankings in TREC RUN format into a TREC CAR Y3 submission

- `y3_validate_submission.py` for validation of TREC CAR Y3 submission files

- `paragraph_id_list.py` to convert a paragraphCorpus.cbor into a list of paragraph id (to speed up validation)


Additionally, this suite provides a library for

- constructing TREC CAR Y3 pages in the correct format (see `trec_car_y3_conversion/y3_data.py`)

- populating pages in a section-by-section fashion (see `trec_car_y3_conversion/page_population.py`)

- loading run files (see `trec_car_y3_conversion/run_file.py`)

- processing paragraph information in bulk (see `trec_car_y3_conversion/paragraph_text_collector.py`)



# Conversion of Rankings into Pages

```
Usage: y3_convert_ranking_to_ordering.py [-h] --outline-cbor OUTLINE_CBOR
                                         --output-directory OUTPUT_DIRECTORY
                                         [--run-directory RUN_DIRECTORY]
                                         [--run-file RUN_FILE]
                                         [--run-name RUN_NAME]
                                         [--compression COMPRESSION]
                                         [--include-text-from-paragraph-cbor INCLUDE_TEXT_FROM_PARAGRAPH_CBOR]
                                         [-k INT]

Converts section-level rankings into a TREC CAR Y3 page. The top ranked
passage for each section heading are concatenated in order to max out the
budget of `top_k` passages per page. No further optimizations are carried out,
meaning that the resulting page may contain duplicate paragraphs, and could
start with a paragraph that is not suitable as introduction. After running the
script, the output-directory will contain multiple *.jsonl files, each named
according to the `run-name` given in the trec run file or overwritten by the
command line argument.

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
  --compression COMPRESSION
                        Compress written file with 'xz', 'bz2', or 'gz'.
  --include-text-from-paragraph-cbor INCLUDE_TEXT_FROM_PARAGRAPH_CBOR
                        If set, loads paragraph text from the paragraph corpus
                        .cbor file.
  -k INT                Maximum number of paragraphs to pull from each query
                        in a runfile. (Default is 20)

```

Example 1: (Minimal TREC CAR Y3 submission)

```
python3 y3_convert_ranking_to_ordering.py --outline-cbor ./benchmarkY3test.public/benchmarkY3test.cbor-outlines.cbor  --output-directory ../populated/  --run-file bm25.run --compression gz --run-name TEAM-bm25
```


Example 2: (Includes text in para_bodies from paragraph-cbor)

```
y3_convert_ranking_to_ordering.py --outline-cbor ./benchmarkY3test.public/benchmarkY3test.cbor-outlines.cbor --output-directory ../populated/  --run-file ../sectionPath-bm25-none.run --run-name TEAM-bm25 --compression gz --include-text-from-paragraph-cbor ./paragraphCorpus/dedup.articles-paragraphs.cbor
```


# Validating TREC CAR Y3 Submission Files

The validation script requires these resource files:

 -  **[paragraph_ids.txt.xz](http://trec-car.cs.unh.edu/datareleases/v2.3/paragraph_ids.txt.xz)** the list of valid paragraph ids in the paragraphCorpus - must either be in the working directory or path given on command line.
 -  **benchmarkY3test.cbor-outlines.cbor** from [benchmarkY3test](http://trec-car.cs.unh.edu/datareleases/v2.3/benchmarkY3test.public.tar.gz).
 - optionally, for the validation of para_bodies, the **dedup.articles-paragraphs.cbor** from the [paragraphCorpus](http://trec-car.cs.unh.edu/datareleases/v2.0/paragraphCorpus.v2.0.tar.xz).


```
usage: y3_validate_submission.py [-h] [--print-validation-rules]
                                 [--json-file JSON_FILE] [--json-dir JSON_DIR]
                                 --outline-cbor OUTLINE_CBOR [-k INT]
                                 [--check-y3] [--check-origins]
                                 [--check-text-from-paragraph-cbor CHECK_TEXT_FROM_PARAGRAPH_CBOR]
                                 [--check-text-from-paragraph-id-list ID-FILE]
                                 [--fail-on-first] [--print-json]
                                 [--submission-check-y3] [--confirm-correct]

Validate JSON-L submission files for TREC CAR Y3 of populated pages against an
outline-cbor file and paragraph cbor file (or optional, paragraph-id-list).
Optional fields such as "para_body" and "paragraph_origins" can be validated
as well. Strict validation for TREC CAR Y3 squids (stable query unique ids)
can be enabled or disabled. The script provides different validation modes,
such as "fail-on-first", "print-json", and "confirm-correct". Validation
errors and warnings will be printed on stdout. If no output is generated, then
the file is correct.

optional arguments:
  -h, --help            show this help message and exit
  --print-validation-rules
                        Print the list validation rules and exit.
  --json-file JSON_FILE
                        Single Json-lines file CAR Y3 format.
  --json-dir JSON_DIR   Directory of Json-lines file CAR Y3 format.
  --outline-cbor OUTLINE_CBOR
                        Path to an outline.cbor file
  -k INT                Maximum number of paragraphs to pull from each query
                        in a runfile. (Default is 20)
  --check-y3            Activate strict checks for TREC CAR Y3 submission
                        (including squid ids).
  --check-origins       Activate strict checks paragraph_origins
  --check-text-from-paragraph-cbor CHECK_TEXT_FROM_PARAGRAPH_CBOR
                        If set, loads and checks paragraph text from the
                        paragraph corpus .cbor file. Remark: This check will
                        be time-consuming.
  --check-text-from-paragraph-id-list ID-FILE
                        If set, loads and checks paragraph-ids with list
                        (*.txt / *.txt.xz). This list contains one id per line
                        and can be produced with paragraph_id_list.py. (Only
                        in effect when --check-text-from-paragraph-cbor is not
                        set.)
  --fail-on-first       If set, fails on first error. (Otherwise, lists all
                        issues)
  --print-json          If set, prints the problematic JSON.
  --submission-check-y3
                        Checks performed during TREC CAR Y3 upload. Equivalent
                        to -k 20 --check-y3 --fail-on-first --check-text-from-
                        paragraph-id-list paragraph_ids.txt.xz
  --confirm-correct     Confirms if the file is correct on stdout. (Otherwise,
                        files are correct when no output is generated on
                        stderr)
```



Example 1 (requires file paragraph_ids.txt.xz in the working directory):

```
python3 y3_validate_submission.py --json-file ../populated/TEAM-bm25.jsonl.gz --submission-check-y3 --outline-cbor ./benchmarkY3test.public/benchmarkY3test.cbor-outlines.cbor
```

Example 2:

```
python3 y3_validate_submission.py --json-file ../populated/TEAM-bm25.jsonl.gz --check-y3 --check-origins --print-json --outline-cbor ./benchmarkY3test.public/benchmarkY3test.cbor-outlines.cbor --check-text-from-paragraph-cbor ../paragraphCorpus/dedup.articles-paragraphs.cbor
```








VALIDATION RULES for TREC CAR Y3:


Minimal Spec Requirements
-------------------------

- all ids (page squid, run id, section path, etc) must be set to non-empty ascii strings.

- additionally: a paragraph_id must be a hexadecimal string of 40 characters that is contained in the paragraphCorpus.cbor

- Paragraphs for a page must be a non-empty list

- The minimal representation of a paragraph is the `paragraph_id`. The para_body element is optional, but if given, it must be correct and agree with the representation in the paragraphCorpus.cbor. Cannot be set of an empty list, instead the entry must not appear in the json.

- A page's `paragraph_origins` are optional, but if given, they must be correct according to the following defition with valid paragraph id and a float-valued `rank_score`. Cannot be set to an empty list, instead must not appear in json.

   - The section_path` must refer to a valid heading id of the page outlines are allowed. These are to be given in the format "squid/heading id". It is strongly recommended to include paragraphs for all headings.

   - Up to 20 paragraphs are allowed per heading. (We strongly encourage to include exactly 20 paragraphs per heading.)

   - The `rank` field is optional, but if given must  must agree with the sort-order of the `rank_score`. Also, the lowest valid number for `rank` is 1 (i.e., highest rank is 1). Ranks must be unique (i.e., no ties).




Further requirements for Y3 submissions
---------------------------------------

- All page squids must start with the proper namespace, i.e., 'tqa2:`. They cannot contain `%20` symbols, because these were only used in Y1 and Y2 -- not in Y3!

- Run ids must not contain more than 15 alpha-numeric characters including "_-.", but cannot start with '.'. (Please include an abbreviation of your team name!)

- Maximal 20 paragraphs can be givem. We strongly encourage to provide exactly 20 paragraphs!



# Producing a list of paragraph ids

This file is also distributed on the TREC CAR website: <http://trec-car.cs.unh.edu>


```
usage: paragraph_id_list.py [-h] -o FILE --paragraph-cbor CBOR

Produce a list of valid paragraph ids from a paragraph-cbor file.

optional arguments:
  -h, --help            show this help message and exit
  -o FILE               Output file
  --paragraph-cbor CBOR
                        If set, loads and checks paragraph text from the
                        paragraph corpus .cbor file. Remark: This check will
                        be time consuming.

```

Example:

```
python3 paragraph_id_list.py --paragraph-cbor ./paragraphCorpus/dedup.articles-paragraphs.cbor -o paragraph_ids.txt.xz
```