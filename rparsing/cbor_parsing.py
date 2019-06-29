#!/usr/bin/python3
import itertools
from abc import abstractmethod
from typing import List, Dict, Set, Iterator, Optional
import argparse
import os
import json
import sys



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

    def __init__(self, paraId, para_body=None):
        self.para_id = paraId
        self.para_body = para_body  # type: Optional[List[ParBody]]

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

    def __str__(self):
        return self.facet_id.__str__()

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


    def __init__(self, squid: str, title: str, run_id: Optional[str], query_facets: List[QueryFacet]) -> None:
        self.query_facets = query_facets  # type: List[QueryFacet]
        self.run_id = run_id
        self.title = title
        self.squid = squid

        # paragraphs retrieved per facet
        self.facet_paragraphs = {} # type: Dict[str,List[Paragraph]]

        # paragraphs get loaded later
        self.pids = set() # type: Set[str]
        self.paragraphs = [] # type: List[Paragraph]
        # paragraph origins
        self.paragraph_origins = None # type: Optional[List[ParagraphOrigin]]



    def add_paragraph_origins(self, origin):
        if self.paragraph_origins is None:
            self.paragraph_origins = []
        self.paragraph_origins.append(origin)


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

    def add_facet_paragraph(self, qid:str, paragraph: Paragraph)->None:
        assert qid.startswith(self.squid), ( "Query id %s does not belong to this page %s"  % (qid, self.squid))

        if qid not in self.facet_paragraphs:
            self.facet_paragraphs[qid]=[]
        self.facet_paragraphs[qid].append(paragraph)


    def populate_paragraphs(self, top_k):
        """
        In a round-robin fashion, select the top ceil(top_k/num_facets) paragraphs from each ranking, as set through :func:`add_facet_paragraph`.

        However, in cases where a ranking does not have enough paragraphs, or facets are missing, or
        where top_k is not divisible by num_facets and we would have either more or less than top_k paragraphs, it is
        difficult to best fill the `top_k` budget.

        This function will use a round-robin approach, iteratively filling the budget by selecting one paragraph from
        each facet, then looping over facets until the budget is filled. This function will do its best to
        exactly meet the top_k budget. Only when there are not enough retrieved paragraphs (submitted through :func:`add_facet_paragraph`)
        this function will not maximize the budget.

        After determining which paragraphs to select, this function will populate the self.paragraphs field  (and self.pids)
        by concatenating the selected paragaphs from self.facet_paragraphs in the order in which facets appear in the
        outline.

        :param top_k:
        :return:
        """

        facetKeys = self.facet_paragraphs.keys()
        for fk in facetKeys:
            assert fk.startswith(self.squid), "Facet of wrong page"


        facet_take_k = {facet_id:0 for facet_id in self.facet_paragraphs}
        k = 0
        did_change = True
        self.paragraphs = []

        while k < top_k and did_change:
            did_change = False
            for facet in self.query_facets:
                facet_id = facet.facet_id
                if k < top_k and facet_id in facet_take_k:
                    if len(self.facet_paragraphs[facet_id]) > (facet_take_k[facet_id] + 1):
                        facet_take_k[facet_id] += 1
                        k += 1
                        did_change = True

        # assert sum (v for v in facet_take_k.values()) > 0, ("no paragraphs for select for page %s" % self.squid)

        for facet in self.query_facets:
            facet_id = facet.facet_id
            if facet_id in facet_take_k:
                ps = self.facet_paragraphs[facet_id][0 : facet_take_k[facet_id]] # type: List[Paragraph]
                self.paragraphs.extend(ps)


        if k == 0:
            print ("Warning: No paragraphs for population of page %s" % (self.squid), file=sys.stderr)
        elif k < top_k:
            print ("Warning: page %s could only be populated with %d paragraphs (instead of full budget %d)" % (self.squid, k, top_k), file=sys.stderr)
        self.pids = {p.para_id for p in self.paragraphs}


def submission_to_json(pages: Iterator[Page]) -> str:
    return "\n".join([json.dumps(page.to_json()) for page in pages])



# ---------------------------- Run Parsing ----------------------------

class RunLine(object):
    def __init__(self, line:str, run_name: Optional[str] = None):
        splits = line.split()
        self.qid = splits[0]             # Query ID
        self.doc_id = splits[2]          # Paragraph ID
        self.rank = int(splits[3])       # Rank of retrieved paragraph
        self.score = float(splits[4])    # Score of retrieved paragraph
        self.run_name = splits[5]        # Name of the run
        if run_name is not None:
            self.run_name = run_name


class RunFile(object):
    """
    Responsible for reading a single runfile, line-by-line, and storing them in RunLine data classes.
    """

    def __init__(self,  top_k:int, run_file:str, run_name:Optional[str] = None):
        self. runlines = [] # type: List[RunLine]
        self.top_k = top_k # type: int
        self.load_run_file(run_file, run_name)

    #
    # def load_run_dir(self,run_dir):
    #     for run_file in os.listdir(run_dir):
    #         self.load_run_file(run_file = run_file, run_name = None)

    def load_run_file(self,run_file, run_name: Optional[str]):
        with open(run_file) as f:
            for line in f:
                run_line = RunLine(line, run_name)
                if (run_line.rank <= self.top_k):
                    self.runlines.append(run_line)



class RunPageKey(object):

    def __eq__(self, o: object) -> bool:
        return isinstance(o, RunPageKey) and self.key.__eq__(o.key)


    def __hash__(self) -> int:
        return self.key.__hash__()

    def __init__(self, run_name, squid):
        self.run_name = run_name
        self.squid = squid
        self.key = (run_name, squid)


    def __str__(self):
        return (self.run_name, self.squid).__str__()

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
            paragraph = Paragraph(paraId=run_line.doc_id)  # create empty paragraph, contents will be loaded later.
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
    parser = argparse.ArgumentParser("Convert TREC Run files into TREC CAR Y3 submission JSON-lines format")
    parser.add_argument("outline_cbor"
                        , help = "Path to an outline.cbor file"
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
        out_name = "jsons/" + run_id + ".json"
        with open(out_name, "w") as f:
            f.write(submission_to_json(pages))

if __name__ == '__main__':
    run_parse()





