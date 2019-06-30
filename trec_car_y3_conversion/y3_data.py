#!/usr/bin/python3
from abc import abstractmethod
from typing import List, Dict, Set, Iterator, Optional
import json
import sys



from trec_car.read_data import iter_outlines


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
    def __init__(self, text:str, entity:Optional[str]=None, link_section:Optional[str]=None, entity_name:Optional[str]=None)-> None:
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

    def __init__(self, paraId:str, para_body:Optional[List[ParBody]]=None)->None:
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
    def __init__(self, facet_id:str, heading:str)->None:
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
    def __init__(self, para_id:str, section_path:str, rank_score:float, rank:int)->None:
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
    def __init__(self, qid:str, doc_id:str, rank:int,score:float,run_name:str) -> None:
        self.qid = qid
        self.doc_id = doc_id
        self.rank = rank
        self.score = score
        self.run_name = run_name


    @staticmethod
    def from_line(line:str, run_name: Optional[str] = None) -> "RunLine":
        splits = line.split()
        qid = splits[0]             # Query ID
        doc_id = splits[2]          # Paragraph ID
        rank = int(splits[3])       # Rank of retrieved paragraph
        score = float(splits[4])    # Score of retrieved paragraph
        if run_name is None:
            run_name = splits[5]        # Name of the run
        return RunLine(qid=qid, doc_id=doc_id, rank=rank, score=score, run_name = run_name)


class RunFile(object):
    """
    Responsible for reading a single runfile, line-by-line, and storing them in RunLine data classes.
    """

    def __init__(self,  top_k:int, run_file:str, run_name:Optional[str] = None)-> None:
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
                run_line = RunLine.from_line(line, run_name)
                if (run_line.rank <= self.top_k):
                    self.runlines.append(run_line)



class RunPageKey(object):

    def __eq__(self, o: object) -> bool:
        return isinstance(o, RunPageKey) and self.key.__eq__(o.key)


    def __hash__(self) -> int:
        return self.key.__hash__()

    def __init__(self, run_name:str, squid:str)-> None:
        self.run_name = run_name
        self.squid = squid
        self.key = (run_name, squid)


    def __str__(self):
        return (self.run_name, self.squid).__str__()
