#!/usr/bin/python3
import itertools
import os
import argparse
import sys

from typing import List, Iterator, Optional, Any, Tuple, Iterable

from trec_car_y3_conversion.page_population import populate_pages, populate_pages_with_page_runs
from trec_car_y3_conversion.run_file import RunFile
from trec_car_y3_conversion.utils import maybe_compressed_open
from trec_car_y3_conversion.y3_data import Page, submission_to_json



def get_parser():
    parser = argparse.ArgumentParser(description="""Converts section-level rankings into a TREC CAR Y3 page. 
                                                     The top ranked passage for each section heading are concatenated
                                                     in order to max out the budget of `top_k` passages per page.
                                                     No further optimizations are carried out, meaning that the
                                                     resulting page may contain duplicate paragraphs, and could start
                                                     with a paragraph that is not suitable as introduction.
                                                                                                          
After running the script, the output-directory will contain multiple *.jsonl files,
each named according to the `run-name` given in the trec run file or overwritten by the command line argument.                                                      
                                                 """)
    parser.add_argument("--outline-cbor"
                        , help = "Path to an outline.cbor file"
                        , required= True
                        )

    parser.add_argument("--output-directory"
                        , help = "Output directory (writes on json file per run)"
                        , required= True
                        )


    parser.add_argument("--run-directory"
                        , help = "Path to a directory containing all runfiles to be parsed (uses run name given in trec run files)."
                        )

    parser.add_argument("--run-file"
                        , help = "Single runfiles to be parsed."
                        )

    parser.add_argument("--run-name"
                        , help = "overwrite run name in run-file with this one."
                        )

    parser.add_argument("--compression"
                        , help = "Compress written file with \'xz\', \'bz2\', or \'gz\'."
                        )

    parser.add_argument("--include-text-from-paragraph-cbor"
                        , help = "If set, loads paragraph text from the paragraph corpus .cbor file."
                        )

    parser.add_argument("-k"
                        , help = "Maximum number of paragraphs to pull from each query in a runfile. (Default is 20)"
                        , default = 20
                        , metavar = "INT"
                        )

    parser.add_argument("--is-page-level-run"
                        , help = "Set of input runs are page-level runs (default: section-level)"
                        , action = "store_true"
                        )

    parser.add_argument("--remove-duplicates"
                        , help = "Remove duplicate passages (only retains first occurrence on page)"
                        , action = "store_true"
                        )

    parsed = parser.parse_args()
    return parsed.__dict__

def group_pages_by_run_id(pages:Iterable[Page]) -> Iterator[Tuple[Any, Iterable[Page]]]:
    def keyfunc(p):
        return p.run_id
    return itertools.groupby(sorted(pages, key=keyfunc), key=keyfunc)


def run_main() -> None:
    parsed = get_parser()
    outlines_cbor_file = parsed["outline_cbor"]  # type: str
    run_dir = parsed["run_directory"]  # type: Optional[str]
    run_file = parsed["run_file"]  # type: Optional[str]
    run_name = parsed["run_name"]  # type: Optional[str]
    ouput_dir = parsed["output_directory"]  # type: str
    compression= parsed["compression"]  # type: Optional[str]
    is_page_level_run= parsed["is_page_level_run"]  # type: bool
    remove_duplicates= parsed["remove_duplicates"]  # type: bool

    top_k = int(parsed["k"]) # type: int
    paragraph_cbor_file = parsed["include_text_from_paragraph_cbor"]  # type: Optional[str]

    if Page.fail_alphanumeric_str(run_name):
        raise RuntimeError("Run name %s of invalid type. Must be non-empty string containing only characters that are alphanumeric or \'-\', \'_\', \'.\' -- however cannot start with \'.\'!"% run_name)

    runs = load_runs(run_dir, run_file, run_name, top_k)

    if is_page_level_run:
        populated_pages = populate_pages_with_page_runs(outlines_cbor_file, runs, top_k, paragraph_cbor_file)
    else:
        populated_pages = populate_pages(outlines_cbor_file, runs, top_k, remove_duplicates, paragraph_cbor_file)

    # Write populated, text filled pages to output directory in JSON format.
    if not os.path.exists(ouput_dir + "/"):
        os.mkdir(ouput_dir + "/")

    for run_id, pages in group_pages_by_run_id(populated_pages):
        out_name = ouput_dir+"/" + run_id + ".jsonl"  + ('.'+compression if compression else '')
        with maybe_compressed_open(out_name, "wt") as f:
            f.write(submission_to_json(pages))
            print("Created file "+out_name,file=sys.stderr)


def load_runs(run_dir:Optional[str], run_file:Optional[str], run_name:Optional[str], top_k:int)-> List[RunFile]:
    runs = []  # type: List[RunFile]
    if run_dir is not None:
        for run_loc in os.listdir(run_dir):
            runs.append(RunFile(top_k=top_k, run_file=run_dir + "/" + run_loc))
    if run_file is not None:
        runs.append(RunFile(top_k=top_k, run_file=run_file, run_name=run_name))
    return runs


if __name__ == '__main__':
    run_main()





