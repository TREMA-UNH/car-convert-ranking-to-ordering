#!/usr/bin/python3
import itertools
from abc import abstractmethod
from typing import List, Dict, Union, Set, Iterator
import argparse
import os
import json
from collections import Counter


from trec_car.read_data import iter_outlines, iter_paragraphs, ParaLink, ParaText


# ---------------------------- CBOR Outline Parser ----------------------------
class OutlineReader(object):

    @staticmethod
    def outline_to_page(outline):
        # todo adjust for hierarchical sections using outline.flat_headings_list
        pageFacets = [QueryFacet(facet_id=outline.page_id+"/"+section.headingId, heading=section.heading) for section in outline.child_sections]

        return Page(squid = outline.page_id, title=outline.page_name, run_id=None, query_facets = pageFacets)


    @staticmethod
    def initialize_pages(f):
        return [OutlineReader.outline_to_page(outline) for outline in iter_outlines(f)]


# ---------------------------- JSON Data Structueres ----------------------------
class Jsonable(object):
    """
    Convenience class that contains method to convert attributes of a class into a json.
    """
    @abstractmethod
    def to_json(self)-> dict:
        pass


class Section(Jsonable):
    pass


class ParBody(Jsonable):
    """
    Represents the text of a paragraph.
    """
    def __init__(self, text, entity=None, link_section=None, entity_name=None):
        self.entity_name = entity_name
        self.link_section = link_section
        self.entity = entity
        self.text = text


    def to_json(self) -> dict:
        if self.entity is None:
            return {"text": self.text}
        else:
            return self.__dict__


class Paragraph(Jsonable):
    """
    Paragraph container that contains links / paragraph text. Is updated using ParagraphTextcollector class.
    """
    para_body = None # type: Union[None, List[ParBody]]

    def __init__(self, paraId, para_body=None):
        self.para_id = paraId
        self.para_body = para_body

    def add_para_body(self, body):
        if self.para_body is None:
            self.para_body = []
        self.para_body.append(body)



    def to_json(self) -> dict:
        if self.para_body is None:
            return {"para_id": self.para_id}
        else:
            return {"para_id": self.para_id
                    , "para_body" : [ body.to_json() for body in self.para_body]
                    }



class QueryFacet(Jsonable):
    """
    An annotation query facet (containing the facet's name and id)
    """
    def __init__(self, facet_id, heading):
        self.facet_id = facet_id
        self.heading = heading

    def to_json(self)-> dict:
        return {"heading": self.heading
                , "heading_id": self.facet_id
                }

class ParagraphOrigin(Jsonable):
    """
    Contains information about the ranking from which a paragraph originates
    """
    def __init__(self, para_id, section_path, rank_score, rank):
        """
        :param para_id:         ID of the paragraph
        :param section_path:    The toplevel section that the paragraph is contained in
        :param rank_score:      The score of the paragraph when it was retrieved
        :param rank:            The rank of the paragraph when it was retrieved
        """
        self.para_id = para_id
        self.section_path = section_path
        self.rank_score = rank_score
        self.rank = rank

    def to_json(self)-> dict:
        return self.__dict__

class Page(Jsonable):
    """
    A page used for annotations.
    """

    # paragraphs get loaded later
    pids = set() # type: Set[str]
    paragraphs = [] # type: List[Paragraph]
    # paragraph origins
    paragraph_origins = None # type: Union[List[ParagraphOrigin], None]

    def __init__(self, squid: str, title: str, run_id: Union[str,None], query_facets: List[QueryFacet]) -> None:
        self.query_facets = query_facets  # type: List[QueryFacet]
        self.run_id = run_id
        self.title = title
        self.squid = squid


    def add_paragraph_origins(self, origin):
        if self.paragraph_origins is None:
            self.paragraph_origins = []
        self.paragraph_origins.append(origin)



    def add_paragraph(self, paragraph: Paragraph):
        if paragraph.para_id not in self.pids:
            self.pids.add(paragraph.para_id)
            self.paragraphs.append(paragraph)


    def copy_prototype(self,run_id):
        return Page(self.squid, self.title, run_id, self.query_facets)


    def to_json(self):
        dictionary =  { "title": self.title
                , "squid": self.squid
                , "run_id": self.run_id
                , "query_facets": [facet.to_json() for facet in self.query_facets]
                , "paragraphs": [para.to_json() for  para in self.paragraphs]
                }
        if self.paragraph_origins is None:
            return dictionary
        else:
            dictionary["paragraph_origins"] = [origin.to_json() for origin in self.paragraph_origins]
            return dictionary



def submission_to_json(pages: Iterator[Page]) -> str:
    return "\n".join([json.dumps(page.to_json()) for page in pages])



# ---------------------------- Run Parsing ----------------------------
class RunPageKey(object):

    def __eq__(self, o: object) -> bool:
        return isinstance(o, RunPageKey) and self.key.__eq__(o.key)


    def __hash__(self) -> int:
        return self.key.__hash__()

    def __init__(self, run_name, squid):
        self.run_name = run_name
        self.squid = squid
        self.key = (run_name, squid)

class RunManager(object):
    """
    Responsible for all the heavy lifting:
     - Parses a directory full of runfiles.
     - Creates data classes (that can be turned into jsons) based on these runs
    """
    paragraphs_to_retrieve = {} # type: Dict[str, List[Paragraph]]
    runs = [] # type: List[RunReader]
    pages = {}  # type: Dict[RunPageKey, Page]

    page_prototypes = {} # type: Dict[str, Page]

    def __init__(self, run_dir: str, outline_cbor_file: str, nlines: int = 20) -> None:

        with open(outline_cbor_file, 'rb') as f:
            self.page_prototypes = {facet.facet_id: page for page in OutlineReader.initialize_pages(f) for facet in page.query_facets}

        for run_loc in os.listdir(run_dir):
            self.runs.append(RunReader(run_dir + "/" + run_loc, nlines=nlines))


        # After parsing run files, convert lines of these files into pages
        for run in self.runs:
            for run_line in run.runlines:
                self.parse_run_line(run_line)


    def parse_run_line(self, run_line):

        if(run_line.qid in self.page_prototypes):   # Ignore other rankings
            page_prototype = self.page_prototypes[run_line.qid]
            squid = page_prototype.squid
            key = RunPageKey(run_name=run_line.run_name, squid=squid)

            # The first time we see a toplevel query for a particular run, we need to initialize a jsonable page
            if key not in self.pages:
                self.pages[key] = page_prototype.copy_prototype(run_line.run_name)

            page = self.pages[key]

            # Add paragraph and register this for later (when we retrieve text / links)
            paragraph = Paragraph(paraId=run_line.doc_id)  # create empty paragraph, contents will be loaded later.
            page.add_paragraph(paragraph)
            self.register_paragraph(paragraph)

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


class RunLine(object):
    def __init__(self, line):
        splits = line.split()
        self.qid = splits[0]             # Query ID
        self.doc_id = splits[2]          # Paragraph ID
        self.rank = int(splits[3])       # Rank of retrieved paragraph
        self.score = float(splits[4])    # Score of retrieved paragraph
        self.run_name = splits[5]        # Name of the run


class RunReader(object):
    """
    Responsible for reading a single runfile, line-by-line, and storing them in RunLine data classes.
    """
    runlines = [] # type: List[RunLine]

    def __init__(self, run_loc, nlines):
        self.run_counts = Counter()
        self.seen_pids = set()  # type: Set[str]

        with open(run_loc) as f:
            for line in f:
                run_line = RunLine(line)
                self.run_counts[run_line.qid] += 1

                # Only store the top N retrieved paragraphs from each query
                if self.run_counts[run_line.qid] > nlines:
                    continue
                self.seen_pids.add(run_line.doc_id)


                self.runlines.append(RunLine(line))



class ParagraphTextCollector(object):
    """
    Retrieves text from paragraphCorpus.cbor file and adds it to the corresponding paragrpahs
    """
    def __init__(self, paragraphs_to_retrieve):
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

    def update_paragraph(self, p: Paragraph, pbodies):
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
    parser = argparse.ArgumentParser()
    outline_arg = parser.add_argument("outline_cbor")
    outline_arg.help = "Path to an outline.cbor file"

    run_directory = parser.add_argument("run_directory")
    run_directory.help = "Path to a directory containing runfiles to be parsed."

    tmap = parser.add_argument("paragraph_cbor")
    tmap.help = "Path to either a paragraph corpus .cbor file."

    n_paragraphs = parser.add_argument("-n")
    n_paragraphs.help = "Maximum number of paragraphs to pull from each query in a runfile. (Default is 10)"
    n_paragraphs.default = 10
    n_paragraphs.metavar = "INT"

    parsed = parser.parse_args()
    return parsed.__dict__


def run_parse() -> None:
    parsed = get_parser()
    outlines_cbor_file = parsed["outline_cbor"]
    run_loc = parsed["run_directory"]
    np = int(parsed["n"])
    paragraph_cbor_file = parsed["paragraph_cbor"]
    run_manager = RunManager(run_loc, outlines_cbor_file, nlines=np)
    run_manager.retrieve_text(paragraph_cbor_file)


    def keyfunc(p):
        return p.run_id

    if not os.path.exists("jsons/"):
        os.mkdir("jsons/")

    for run_id, pages in itertools.groupby(sorted(run_manager.pages.values(), key=keyfunc), key=keyfunc):
        out_name = "jsons/" + run_id + ".json"
        with open(out_name, "w") as f:
            f.write(submission_to_json(pages))

    # for (k,v) in sorted(run_manager.pages.items(), key=lambda x: x[0]):
    #     v.write_self()


if __name__ == '__main__':
    run_parse()





