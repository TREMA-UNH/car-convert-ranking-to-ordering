#!/usr/bin/python3
import itertools
from typing import Union
import argparse
import os

from trec_car.read_data import iter_paragraphs, ParaText, ParaLink
from trec_car_y3_conversion.y3_data import *


class RunManager(object):
    """
    Responsible for all the heavy lifting:
     - Parses a directory full of runfiles.
     - Creates data classes (that can be turned into jsons) based on these runs
    """

    def __init__(self, outline_cbor_file: str, top_k: int = 20, run_dir: Optional[str] = None, run_file: Optional[str] = None, run_name: Optional[str] = None) -> None:
        self.paragraphs_to_retrieve = {} # type: Dict[str, List[Paragraph]]
        self.runs = [] # type: List[RunFile]
        self.pages = {}  # type: Dict[RunPageKey, Page]
        self.page_prototypes = {} # type: Dict[str, Page]

        with open(outline_cbor_file, 'rb') as f:
            for page in OutlineReader.initialize_pages(f):
                for facet in page.query_facets:
                    self.page_prototypes[facet.facet_id] = page

            # self.page_prototypes = {facet.facet_id: page for page in OutlineReader.initialize_pages(f) for facet in page.query_facets}


        if run_dir is not None:
            for run_loc in os.listdir(run_dir):
                self.runs.append(RunFile(top_k=top_k, run_file = run_dir + "/" + run_loc))
        if run_file is not None:
            self.runs.append(RunFile(top_k=top_k, run_file = run_file, run_name = run_name))


        # After parsing run files, convert lines of these files into pages
        for run in self.runs:
            for run_line in run.runlines:
                self.parse_run_line(run_line)


    def parse_run_line(self, run_line: RunLine) -> None:

        if(run_line.qid in self.page_prototypes):   # Ignore other rankings
            page_prototype = self.page_prototypes[run_line.qid]
            squid = page_prototype.squid
            assert run_line.qid.startswith(squid), "fetched wrong page prototype"

            key = RunPageKey(run_name=run_line.run_name, squid=squid)

            # The first time we see a toplevel query for a particular run, we need to initialize a jsonable page
            if key not in self.pages:
                self.pages[key] = page_prototype.copy_prototype(run_line.run_name)

            page = self.pages[key]
            assert run_line.qid.startswith(page.squid), "fetched wrong page"

            # Add paragraph and register this for later (when we retrieve text / links)
            paragraph = Paragraph(para_id=run_line.doc_id)  # create empty paragraph, contents will be loaded later.
            page.add_facet_paragraph(run_line.qid, paragraph)
            assert run_line.qid.startswith(page.squid), "adding paragraphs to wrong page"
            # self.register_paragraph(paragraph)

            # Also add which query this paragraph is with respect to
            origin = ParagraphOrigin(
                para_id=run_line.doc_id,
                rank=run_line.rank,
                rank_score=run_line.score,
                section_path=run_line.qid
            )
            page.add_paragraph_origins(origin)



    def register_paragraph(self, paragraph: Paragraph):
        """
        Remember where this paragraph is for later when we have to parse a paragraphCorpus.cbor file.
        We will be adding the parsed text to this paragraph. Since there can be multiple instances of this paragraph
        among queries and runs, they are all stored in a map of lists for later text retrieval.
        """
        key = paragraph.para_id
        if key not in self.paragraphs_to_retrieve:
            self.paragraphs_to_retrieve[key] = []
        self.paragraphs_to_retrieve[key].append(paragraph)


    def retrieve_text(self, paragraph_cbor_file):
        """
        Passes all registered paragraphs to the ParagraphTextCollector for text retrieval.
        :param paragraph_cbor_file: Location to paragraphCorpus.cbor file
        """
        pcollector = ParagraphTextCollector(self.paragraphs_to_retrieve)
        pcollector.retrieve_paragraph_mappings(paragraph_cbor_file)

    # def cut_pages_to_top_k(self, top_k):
    #     for (key, page)  in self.pages.items():
    #         page.populate_paragraphs(top_k)



class ParagraphTextCollector(object):
    """
    Retrieves text from paragraphCorpus.cbor file and adds it to the corresponding paragrpahs
    """
    def __init__(self, paragraphs_to_retrieve:Dict[str, List[Paragraph]])-> None:
        self.paragraphs_to_retrieve = paragraphs_to_retrieve


    def retrieve_paragraph_mappings(self, paragraph_cbor_file):
        """
        :param paragraph_cbor_file: Location of the paragraphCorpus.cbor file
        """
        counter = 0
        seen = 0
        total = len(self.paragraphs_to_retrieve)
        with open(paragraph_cbor_file, 'rb') as f:
            for p in iter_paragraphs(f):
                counter += 1
                if counter % 100000 == 0:
                    print("(Searching paragraph cbor): {}".format(counter))

                if p.para_id in self.paragraphs_to_retrieve:
                    for p_to_be_updated in self.paragraphs_to_retrieve[p.para_id]:
                        self.update_paragraph(p_to_be_updated, p.bodies)

                    seen += 1
                    if seen == total:
                        break

    def update_paragraph(self, p: Paragraph, pbodies:List[Union[ParaLink, ParaText]]):
        """
        :param p: Paragraph that we will be updating
        :param pbodies:
        """
        for body in pbodies:
            if isinstance(body, ParaLink):
                body = ParBody(text=body.anchor_text, entity=body.pageid, link_section=body.link_section, entity_name=body.page)
            elif isinstance(body, ParaText):
                body = ParBody(text=body.get_text())

            p.add_para_body(body)



def get_parser():
    parser = argparse.ArgumentParser("y3_convert_ranking_to_ordering.py")
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

    parser.add_argument("--include-text-from-paragraph-cbor"
                        , help = "If set, loads paragraph text from the paragraph corpus .cbor file."
                        )

    parser.add_argument("-k"
                        , help = "Maximum number of paragraphs to pull from each query in a runfile. (Default is 10)"
                        , default = 10
                        , metavar = "INT"
                        )

    parsed = parser.parse_args()
    return parsed.__dict__


def run_parse() -> None:
    parsed = get_parser()
    outlines_cbor_file = parsed["outline_cbor"]  # type: str
    run_dir = parsed["run_directory"]  # type: Optional[str]
    run_file = parsed["run_file"]  # type: Optional[str]
    run_name = parsed["run_name"]  # type: Optional[str]
    ouput_dir = parsed["output_directory"]  # type: str

    top_k = int(parsed["k"]) # type: int
    paragraph_cbor_file = parsed["include_text_from_paragraph_cbor"]  # type: Optional[str]



    run_manager = RunManager(outline_cbor_file = outlines_cbor_file, top_k = top_k, run_dir=run_dir, run_file = run_file, run_name = run_name)

    for page in run_manager.pages.values():
        page.populate_paragraphs(top_k)
        for para in page.paragraphs:
            run_manager.register_paragraph(para)
            # Register the paragraph here.
            # the paragraph text will be set directly into the paragraph object by the RunManager

    if (paragraph_cbor_file is not None):
        run_manager.retrieve_text(paragraph_cbor_file)


    def keyfunc(p):
        return p.run_id

    if not os.path.exists("jsons/"):
        os.mkdir("jsons/")

    for run_id, pages in itertools.groupby(sorted(run_manager.pages.values(), key=keyfunc), key=keyfunc):
        out_name = ouput_dir+"/" + run_id + ".jsonl"
        with open(out_name, "w") as f:
            f.write(submission_to_json(pages))

if __name__ == '__main__':
    run_parse()





