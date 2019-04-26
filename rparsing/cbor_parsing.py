#!/usr/bin/python3
from typing import List, Any, Dict, Tuple
import argparse
import os
import json
from collections import Counter


from trec_car.read_data import iter_outlines, iter_paragraphs, ParaLink, ParaText
import sys


class OutlineReader(object):
    page_title_map = ...  # type: Dict[str, str]
    section_heading_map = ...  # type: Dict[str, List[str]]
    section_id_map = ...  # type: Dict[str, str]

    def __init__(self, f):
        self.page_title_map = {}
        self.section_heading_map = {}
        self.section_id_map = {}

        for outline in iter_outlines(f):
            self.page_title_map[outline.page_id] = outline.page_name
            hmap = []
            imap = []

            for s in outline.child_sections:
                hmap.append(s.heading)
                id = outline.page_id + "/" + s.headingId
                imap.append(id)
                self.page_title_map[id] = s.heading


            self.section_heading_map[outline.page_id] = hmap
            self.section_id_map[outline.page_id] = imap





class Jsonable(object):
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class Section(Jsonable):
    pass


class ParBody(Jsonable):
    def __init__(self):
        self.text = "Test"


class Paragraph(Jsonable):
    para_body = ...  # type: List[ParBody]

    def __init__(self, paraId):
        self.para_id = paraId
        # self.paraBody = [ParBody()]
        self.para_body = []

    def to_json(self):
        self.para_body = [json.loads(i.to_json()) for i in self.para_body]
        return super().to_json()


class QueryFacet(Jsonable):
    def __init__(self, heading_id, heading):
        self.heading_id = heading_id
        self.heading = heading

class ParagraphOrigin(Jsonable):
    def __init__(self, para_id, section_path, rank_score, rank):
        self.para_id = para_id
        self.section_path = section_path
        self.rank_score = rank_score
        self.rank = rank

class Page(Jsonable):
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
        out_name = "jsons/" + self.run_id + "_" + self.title.replace(" ", "_") + ".json"
        if not os.path.exists("jsons/"):
            os.mkdir("jsons/")

        with open(out_name, "w") as f:
            f.write(self.to_json())

class RunLine(object):
    def __init__(self, line):
        e = line.split()
        self.qid = e[0]
        self.doc_id = e[2]
        self.rank = int(e[3])
        self.score = float(e[4])
        self.run_name = e[5]

    def jsonify(self):
        pass


class RunManager(object):
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






    def parse_run_line(self, run_line: RunLine):
        e = run_line.qid.split("/")
        if len(e) != 2:
            return

        tid = e[0]
        key = (run_line.run_name, tid)

        if key not in self.pages:
            p = Page()
            p.squid = tid
            p.title = self.oreader.page_title_map[tid]
            p.run_id = run_line.run_name
            for (f_heading, f_id) in zip(self.oreader.section_heading_map[tid], self.oreader.section_id_map[tid]):
                qf = QueryFacet(heading=f_heading, heading_id=f_id)
                p.query_facets.append(qf)
            self.pages[key] = p

        page = self.pages[key]

        # Add paragraph and register this for later (when we retrieve text / links)
        paragraph = Paragraph(paraId=run_line.doc_id)
        page.add_paragraph(paragraph)
        self.register_paragraph(paragraph)

        # Also add which query this paragraph is with respect to
        origin = ParagraphOrigin(
            para_id=run_line.doc_id,
            rank=run_line.rank,
            rank_score=run_line.score,
            # section_path=self.oreader.page_title_map[run_line.qid]
            section_path=run_line.qid
        )
        page.paragraph_origins.append(origin)

    def register_paragraph(self, paragraph: Paragraph):
        key = paragraph.para_id
        if key not in self.paragraphs_to_retrieve:
            self.paragraphs_to_retrieve[key] = []
        self.paragraphs_to_retrieve[key].append(paragraph)


    def retrieve_text(self, cbor_loc):
        pcollector = ParagraphTextCollector(self.paragraphs_to_retrieve)
        # for run in self.runs:
        #     pcollector.collect_pids(run)

        text_mappings = pcollector.retrieve_paragraph_mappings(cbor_loc)
        # print("Found {} out of {}".format(len(pcollector.text_mappings), len(pcollector.pids)))

        # for page in self.pages.values():
        #     for paragraph in page.paragraphs:
        #          # TODO: change this when we implement entity links in parabody
        #         paragraph.paraBody[0].text = text_mappings[paragraph.paraId]


class RunReader(object):
    runlines = ...  # type: List[RunLine]

    def __init__(self, run_loc, nlines):
        self.runlines = []
        self.run_counts = Counter()
        self.seen_pids = set()

        with open(run_loc) as f:
            for line in f:
                run_line = RunLine(line)
                self.run_counts[run_line.qid] += 1
                if self.run_counts[run_line.qid] > nlines:
                    continue
                self.seen_pids.add(run_line.doc_id)


                self.runlines.append(RunLine(line))



class ParagraphTextCollector(object):
    def __init__(self, paragraphs_to_retrieve):
        self.paragraphs_to_retrieve = paragraphs_to_retrieve

    # def collect_paragraphs(self, manager: RunManager):
    #     for page in run_manager.pages
    #     self.pids = self.pids.union(run.seen_pids)

    # def retrieve_json_mappings(self, json_loc):
    #     with open("backup.json") as f:
    #         text = f.read()
    #     text_mappings = json.loads(text)
    #     return text_mappings
    #
    # def retrieve_cbor_mappings(self, cbor_loc):
    #     counter = 0
    #     with open(cbor_loc, 'rb') as f:
    #         for p in iter_paragraphs(f):
    #             counter += 1
    #             if counter % 100000 == 0:
    #                 print(counter)
    #             if p.para_id in self.pids:
    #                 self.text_mappings[p.para_id] = p.get_text()
    #
    #     with open("backup.json", "w") as f:
    #         f.write(json.dumps(self.text_mappings))
    #     return self.text_mappings
    #
    # def retrieve_text(self, file_loc):
    #     extension = file_loc.split(".")[-1].lower()
    #     if extension == "cbor":
    #         return self.retrieve_cbor_mappings(file_loc)
    #     elif extension == "json":
    #         return self.retrieve_json_mappings(file_loc)
    #
    #     raise Exception("unknown file extension (.{}). Expected .json or .cbor".format(extension))


    def retrieve_paragraph_mappings(self, cbor_loc):
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

        # with open("backup.json", "w") as f:
        #     f.write(json.dumps(self.text_mappings))
        # return self.text_mappings


    def update_paragraph(self, p: Paragraph, pbodies):
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

def test(cbor_loc):
    with open(cbor_loc, 'rb') as f:
        p = next(iter_paragraphs(f))
        print(p.bodies)
        for e in p.bodies:
            if isinstance(e, ParaLink):
                print(e.page)
                print(e.get_text())
                print(e.anchor_text)



if __name__ == '__main__':
    parsed = get_parser()
    outlines = parsed["outline_cbor"]
    run_loc = parsed["run_directory"]
    np = int(parsed["n"])
    cbor_loc = parsed["paragraph_cbor"]



    # path = "/home/hcgs/PycharmProjects/run_parsing/benchmarkY2.cbor-outlines.cbor"
    # run_loc = "/home/hcgs/data_science/data/y2_runs/guir"
    # run_loc2 = "/home/hcgs/data_science/data/y2_runs"
    # cbor_loc = "/home/hcgs/data_science/data/corpus/dedup.articles-paragraphs.cbor"

    # with open(path, 'rb') as f:
        # title_map, section_map = get_smap(f)
        # oreader = OutlineReader(f)
    # print(oreader.section_id_map)

    # run = RunReader(run_loc)
    # run_manager = RunManager("/home/hcgs/data_science/data/small_runs", path, nlines=20)
    run_manager = RunManager(run_loc, outlines, nlines=np)
    run_manager.retrieve_text(cbor_loc)

    # for page in run_manager.pages.values():
    #     for paragraph in page.paragraphs:
    #         if paragraph.paraBody:
    #             print(paragraph.to_json())
    # for i in run_manager.pages.values():
    #     for o in i.paragraph_origins:
    #         pass
    #     break
    # print(run.runlines[0])

    # p = Page()
    # p.squid = "Haha"
    # print(p.to_json())

    # hah = ParagraphTextCollector()
    # test(cbor_loc)
    # print(hah.retrieve_text("blah.dah"))


    # with open("backup.json") as f:
    #     text = f.read()
    #
    # text_map = json.loads(text)



    # for (k,v) in sorted(run_manager.pages.items(), key=lambda x: x[0]):
    #     for paragraph in v.paragraphs:
    #         paragraph.paraBody[0].text = text_map[paragraph.paraId]
    #
    #
    for (k,v) in sorted(run_manager.pages.items(), key=lambda x: x[0]):
        v.write_self()







