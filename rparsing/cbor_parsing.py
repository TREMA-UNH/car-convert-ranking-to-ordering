#!/usr/bin/python3
import itertools
from typing import List, Any, Dict, Tuple
import argparse
import os
import json
from collections import Counter


from trec_car.read_data import iter_outlines, iter_paragraphs, ParaLink, ParaText
import sys

# Removed labelState
# facetState now has "relevance" value

# Hidden state will be renamed to nonrelevantState
# Won't have a second value ('false')


# ---------------------------- CBOR Outline Parser ----------------------------
class OutlineReader(object):
    page_title_map = ...                    # type: Dict[str, str]
    page_toplevel_section_names = ...       # type: Dict[str, List[str]]
    page_toplevel_section_ids = ...         # type: Dict[str, str]

    def __init__(self, f):
        """
        :type outline_loc: Location of .cbor outline file
        """
        self.page_title_map = {}
        self.page_toplevel_section_names = {}
        self.page_toplevel_section_ids = {}

        # Iterate of .cbor file (each item is a page outline)
        for outline in iter_outlines(f):
            self.page_title_map[outline.page_id] = outline.page_name
            toplevel_section_names = []
            toplevel_section_ids = []

            # Contains the top-level sections as children of the page outline
            for section_outline in outline.child_sections:
                toplevel_section_names.append(section_outline.heading)
                id = outline.page_id + "/" + section_outline.headingId
                toplevel_section_ids.append(id)
                self.page_title_map[id] = section_outline.heading

            self.page_toplevel_section_names[outline.page_id] = toplevel_section_names
            self.page_toplevel_section_ids[outline.page_id] = toplevel_section_ids


# ---------------------------- JSON Data Structueres ----------------------------
class Jsonable(object):
    """
    Convenience class that contains method to convert attributes of a class into a json.
    """
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class Section(Jsonable):
    pass


class ParBody(Jsonable):
    """
    Represents the text of a paragraph.
    """
    def __init__(self):
        self.text = "Test"


class Paragraph(Jsonable):
    """
    Paragraph container that contains links / paragraph text. Is updated using ParagraphTextcollector class.
    """
    para_body = ...  # type: List[ParBody]

    def __init__(self, paraId):
        self.para_id = paraId
        # self.paraBody = [ParBody()]
        self.para_body = []

    def to_json(self):
        self.para_body = [json.loads(i.to_json()) for i in self.para_body]
        return super().to_json()


class QueryFacet(Jsonable):
    """
    An annotation query facet (containing the facet's name and id)
    """
    def __init__(self, heading_id, heading):
        self.heading_id = heading_id
        self.heading = heading

class ParagraphOrigin(Jsonable):
    """
    Contains information about the origin of a paragraph
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

class Page(Jsonable):
    """
    A page used for annotations.
    """
    query_facets = ...  # type: List[QueryFacet]
    paragraphs = ... # type: List[Paragraph]
    paragraph_origins = ... # type: List[ParagraphOrigin]

    def __init__(self):
        self.pids = set()
        self.query_facets = []
        self.run_id = ""
        self.title = ""
        self.squid = ""
        self.paragraphs = []
        self.paragraph_origins = []


    def to_json(self):
        self.paragraphs = [json.loads(i.to_json()) for i in self.paragraphs]
        self.query_facets = [json.loads(i.to_json()) for i in self.query_facets]
        self.paragraph_origins = [json.loads(i.to_json()) for i in self.paragraph_origins]
        delattr(self, "pids")
        return super().to_json()


    def add_paragraph(self, paragraph: Paragraph):
        if paragraph.para_id not in self.pids:
            self.pids.add(paragraph.para_id)
            self.paragraphs.append(paragraph)

    def write_self(self):
        """
        Calls to_json recursively on all of the page's components and then writes contents to a file.
        """
        out_name = "jsons/" + self.run_id + "_" + self.title.replace(" ", "_") + ".json"
        if not os.path.exists("jsons/"):
            os.mkdir("jsons/")

        with open(out_name, "w") as f:
            f.write(self.to_json())


class Submission(Jsonable):
    """ A submission for one system/run """
    submission_data = [] # type:[Page]

    def __init__(self, submission_data):
        self.submission_data = [json.loads(i.tojson()) for i in submission_data]



# class MisoAnnotationReader(object):
#     def __init__(self, anno_loc):
#         with open (anno_loc) as f:
#             saved_data = self.parse_annotations(f)
#             print(saved_data.keys())
#
#         query_facet_map = self.create_facet_map(saved_data)
#         print(query_facet_map)
#
#     def create_facet_map(self, saved_data):
#         facets = saved_data["facetState"]
#         query_facet_map = {}
#         for (entry, facet) in facets:
#             pid = entry["paragraphId"]
#             qid = entry["queryId"]
#             heading = facet["heading"]
#
#             if qid not in query_facet_map:
#                 query_facet_map[qid] = {}
#             facet_map = query_facet_map[qid]
#
#             if heading not in facet_map:
#                 facet_map[heading] = set()
#             facet_map[heading].add(pid)
#
#         return query_facet_map
#
#
#     def create_transition_map(self, saved_data):
#         pass
#
#
#
#     def parse_annotations(self, f):
#         json_text = f.read()
#         annotations = json.loads(json_text)
#         return annotations["savedData"]



# ---------------------------- Run Parsing ----------------------------

class RunManager(object):
    """
    Responsible for all the heavy lifting:
     - Parses a directory full of runfiles.
     - Creates data classes (that can be turned into jsons) based on these runs
    """
    paragraphs_to_retrieve = ...  # type: Dict[str, List[Paragraph]]
    runs = ... # type: List[RunReader]
    pages = ...  # type: Dict[Tuple[str, str], Page]

    def __init__(self, run_dir, cbor_loc, nlines=20):
        self.runs = []
        self.pages = {}
        self.paragraphs_to_retrieve = {}
        with open(cbor_loc, 'rb') as f:
            self.oreader = OutlineReader(f)

        for run_loc in os.listdir(run_dir):
            self.runs.append(RunReader(run_dir + "/" + run_loc, nlines=nlines))


        # After parsing run files, convert lines of these files into pages
        for run in self.runs:
            for run_line in run.runlines:
                self.parse_run_line(run_line)


    def parse_run_line(self, run_line: 'RunLine'):
        e = run_line.qid.split("/")  # todo: this gives incorrect results , see issue #6

        # Skip queries that are not top-level!
        if len(e) != 2:
            return

        tid = e[0]  # toplevel query ID
        key = (run_line.run_name, tid)

        # The first time we see a toplevel query for a particular run, we need to initialize a jsonable page
        if key not in self.pages:
            p = Page()
            p.squid = tid
            p.title = self.oreader.page_title_map[tid]
            p.run_id = run_line.run_name
            for (f_heading, f_id) in zip(self.oreader.page_toplevel_section_names[tid],
                                         self.oreader.page_toplevel_section_ids[tid]):
                qf = QueryFacet(heading=f_heading, heading_id=f_id)
                p.query_facets.append(qf)
            self.pages[key] = p

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
            # section_path=self.oreader.page_title_map[run_line.qid]
            section_path=run_line.qid      # todo fix, see issue #6
        )
        page.paragraph_origins.append(origin)

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


    def retrieve_text(self, cbor_loc):
        """
        Passes all registered paragraphs to the ParagraphTextCollector for text retrieval.
        :param cbor_loc: Location to paragraphCorpus.cbor file
        """
        pcollector = ParagraphTextCollector(self.paragraphs_to_retrieve)
        pcollector.retrieve_paragraph_mappings(cbor_loc)


class RunLine(object):
    def __init__(self, line):
        e = line.split()
        self.qid = e[0]             # Query ID
        self.doc_id = e[2]          # Paragraph ID
        self.rank = int(e[3])       # Rank of retrieved paragraph
        self.score = float(e[4])    # Score of retrieved paragraph
        self.run_name = e[5]        # Name of the run


class RunReader(object):
    """
    Responsible for reading a single runfile, line-by-line, and storing them in RunLine data classes.
    """
    runlines = ...  # type: List[RunLine]

    def __init__(self, run_loc, nlines):
        self.runlines = []
        self.run_counts = Counter()
        self.seen_pids = set()

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


    def retrieve_paragraph_mappings(self, cbor_loc):
        """
        :param cbor_loc: Location of the paragraphCorpus.cbor file
        """
        counter = 0
        seen = 0
        total = len(self.paragraphs_to_retrieve)
        with open(cbor_loc, 'rb') as f:
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
        :param p_to_be_updated: Paragraph that we will be updating
        :param pbodies:
        """
        for e in pbodies:
            body = ParBody()

            if isinstance(e, ParaLink):
                body.text = e.anchor_text
                body.entity = e.pageid
                body.link_section = e.link_section
                body.entity_name = e.page
            elif isinstance(e, ParaText):
                body.text = e.get_text()
            p.para_body.append(body)



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

# def test(cbor_loc):
#     with open(cbor_loc, 'rb') as f:
#         p = next(iter_paragraphs(f))
#         print(p.bodies)
#         for e in p.bodies:
#             if isinstance(e, ParaLink):
#                 print(e.page)
#                 print(e.get_text())
#                 print(e.anchor_text)


def run_parse():
    parsed = get_parser()
    outlines = parsed["outline_cbor"]
    run_loc = parsed["run_directory"]
    np = int(parsed["n"])
    cbor_loc = parsed["paragraph_cbor"]
    run_manager = RunManager(run_loc, outlines, nlines=np)
    run_manager.retrieve_text(cbor_loc)


    def keyfunc(p):
        return p.run_id

    if not os.path.exists("jsons/"):
        os.mkdir("jsons/")

    for run_id, pages in itertools.groupby(sorted(run_manager.pages.items(), key=keyfunc), key=keyfunc):
        out_name = "jsons/" + run_id + ".json"
        with open(out_name, "w") as f:
            f.write(Submission(pages).to_json())

    # for (k,v) in sorted(run_manager.pages.items(), key=lambda x: x[0]):
    #     v.write_self()


if __name__ == '__main__':
    run_parse()





