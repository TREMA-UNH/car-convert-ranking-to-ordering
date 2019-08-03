#!/usr/bin/python3
import itertools
import json
import os
import argparse
import sys

from typing import List, Iterator, Optional, Any, Tuple, Iterable, Dict

from trec_car_y3_conversion.page_population import populate_pages, populate_pages_with_page_runs, ParagraphFiller
from trec_car_y3_conversion.run_file import RunFile
from trec_car_y3_conversion.utils import maybe_compressed_open, safe_group_by
from trec_car_y3_conversion.y3_data import Page, submission_to_json, OutlineReader


def get_parser():
    parser = argparse.ArgumentParser(description="""Loads the paragraph contents for populated pages (*jsonl)                                               
                                                 """)



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

    parser.add_argument("--include-text-from-paragraph-cbor"
                        , help = "If set, loads paragraph text from the paragraph corpus .cbor file."
                        , required=True
                        )

    parser.add_argument("--compression"
                        , help = "Compress written file with \'xz\', \'bz2\', or \'gz\'."
                        )

    parser.add_argument("--outline-cbor"
                        , help = "Path to an outline.cbor file"
                        , required= True
                        )

    parsed = parser.parse_args()
    return parsed.__dict__

def group_pages_by_run_id(pages:Iterable[Page]) -> Iterator[Tuple[Any, Iterable[Page]]]:
    def keyfunc(p):
        return p.run_id
    return itertools.groupby(sorted(pages, key=keyfunc), key=keyfunc)


def load_paragraph_text(pages_per_run:Dict[str,List[Page]], paragraph_cbor_file:str)->None:
        run_manager = ParagraphFiller()
        for pages in pages_per_run.values():
            for page in pages:
                for para in page.paragraphs:
                    run_manager.register_paragraph(para)
        run_manager.retrieve_text(paragraph_cbor_file)


def load_pages(json_loc:str)->List[Page]:
    with maybe_compressed_open(json_loc) as f:
        pages = [] #type: List[Page]
        for line in f:
            try:
                page = Page.from_json(json.loads(line))
                pages.append(page)
            except Exception as x:
                raise x
        return pages


def fill_rank_origins(pages_per_run:Dict[str,List[Page]]) -> None:
    for pages in pages_per_run.values():
        for page in pages:
            if any(orig.rank is None for orig in page.paragraph_origins):
                for section_path, origins in safe_group_by([(orig.section_path, orig) for orig in page.paragraph_origins]).items():
                    origins1 = sorted(origins, key = lambda o: o.rank_score, reverse = True)
                    for o, rank in zip(origins1, range(1,len(origins1)+1)):
                        o.rank = rank


def run_main() -> None:
    parsed = get_parser()
    json_dir = parsed["run_directory"]  # type: Optional[str]
    json_file = parsed["run_file"]  # type: Optional[str]
    ouput_dir = parsed["output_directory"]  # type: str
    paragraph_cbor_file = parsed["include_text_from_paragraph_cbor"]  # type: str
    compression= parsed["compression"]  # type: Optional[str]
    outlines_cbor_file = parsed["outline_cbor"]  # type: str

    page_prototypes = {} # type: Dict[str,Page]
    with open(outlines_cbor_file, 'rb') as f:
        for page in OutlineReader.initialize_pages(f):
            page_prototypes[page.squid] = page



    pages_per_run = {} # type: Dict[str,List[Page]]

    if json_dir:
        for json_loc in os.listdir(json_dir):
            if json_loc.endswith(".jsonl") or ".jsonl." in json_loc:
                try:
                    pages = load_pages(json_dir+os.sep+json_loc)
                    pages_per_run[os.path.basename(json_loc)]=pages
                except Exception as x:
                    print(x, file=sys.stderr)

    if json_file:
        pages = load_pages(json_file)
        pages_per_run[os.path.basename(json_file)]=pages


    load_paragraph_text(pages_per_run=pages_per_run, paragraph_cbor_file= paragraph_cbor_file)

    load_page_facets_and_title(page_prototypes, pages_per_run)

    fill_rank_origins(pages_per_run)

    # Write populated, text filled pages to output directory in JSON format.
    if not os.path.exists(ouput_dir + "/"):
        os.mkdir(ouput_dir + "/")

    for xx, pages in pages_per_run.items():
        run_id = pages[0].run_id
        out_name = ouput_dir+"/" + run_id + ".jsonl"  + ('.'+compression if compression else '')
        with maybe_compressed_open(out_name, "wt") as f:
            f.write(submission_to_json(pages))
            print("Created file "+out_name,file=sys.stderr)


def load_page_facets_and_title(page_prototypes, pages_per_run):
    for pages in pages_per_run.values():
        for page in pages:
            prototype = page_prototypes[page.squid]
            page.query_facets = prototype.query_facets
            page.title = prototype.title


if __name__ == '__main__':
    run_main()





