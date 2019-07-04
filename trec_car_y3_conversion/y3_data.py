#!/usr/bin/python3
from abc import abstractmethod
from typing import List, Dict, Set, Iterator, Optional, Any, TextIO, Tuple, Union
import json
import sys
import pprint



from trec_car.read_data import iter_outlines

def safe_group_by(pairs:Iterator[Tuple[str,Any]])->Dict[str,List[Any]]:
    res = {} # type: Dict[str,List[Any]]
    for (k,v) in pairs:
        if k  not in res:
            res[k] = []
        res[k].append(v)
    return res



# ---------------------------- CBOR Outline Parser ----------------------------
class OutlineReader(object):

    @staticmethod
    def outline_to_page(outline):
        # todo adjust for hierarchical sections using outline.flat_headings_list
        query_facets = [QueryFacet(facet_id=outline.page_id+"/"+section.headingId, heading=section.heading) for section in outline.child_sections]

        return Page(squid = outline.page_id, title=outline.page_name, run_id=None, query_facets = query_facets)



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

    @staticmethod
    def from_json(dict) -> "Jsonable":
        raise RuntimeError("Must call from_json of implementing class")


# ---------------------------- Validation Errors and Warnings ----------------------------

class JsonParsingError(BaseException):
    """ JSON parsing failure. """
    def __init__(self, message, data):
        self.message = "ERROR: "+message
        super(JsonParsingError, self).__init__(self.message + "Problematic JSON:\n"+ (pprint.pformat(data, indent = 4, width = 80)))
        self.data = data

    def problematic_json(self):
        return "Problematic JSON:\n"+ (pprint.pformat(self.data, indent = 4, width = 80))

    def get_msg(self):
        return self.message

    def get_squid(self):
        if 'squid' in self.data:
            return self.data['squid']
        else:
            return "???"




class ValidationError(BaseException):
    """ Data validation failed. """
    def __init__(self, message:str, data:"Page")->None:
        self.message = "ERROR: " +message
        super(ValidationError, self).__init__(self.message+ "Problematic JSON:\n"+ (pprint.pformat(data.to_json(suppress_validation = True), indent = 4, width = 80)))
        self.data = data
        self.squid = data.squid

    def get_msg(self):
        return self.message

    def problematic_json(self):
        return "Problematic JSON:\n"+ (pprint.pformat(self.data.to_json(), indent = 4, width = 80))

    def get_squid(self):
        return self.squid

class ValidationWarning(BaseException):
    """ Data validation warning. """
    def __init__(self, message:str, data:"Page")->None:
        self.message = "WARNING: "+message
        super(ValidationWarning, self).__init__(self.message+ "Problematic JSON:\n"+ (pprint.pformat(data, indent = 4, width = 80)))
        self.data = data
        self.squid = data.squid

    def get_msg(self):
        return self.message

    def problematic_json(self):
        return "Problematic JSON:\n"+ (pprint.pformat(self.data.to_json(), indent = 4, width = 80))

    def get_squid(self):
        return self.squid


# ---------------------------- Json validation helper methods ----------------------------


def optKey(data:Dict[str,Any], key:str)->Optional[Any]:
    if key not in data:
        return None
    else:
        return data[key]

def getKey(data:Dict[str,Any], key:str)->Any:
    if key not in data:
        raise JsonParsingError("Key \'%s\' is not in json dictionary. " % key, data)
    else:
        return data[key]

def getListKey(data:Dict[str,Any], key:str)->List[Any]:
    if key not in data:
        raise JsonParsingError("Key \'%s\' is not in json dictionary. " % key, data)
    if (not isinstance(data[key], list)):
        raise JsonParsingError("Key \'%s\' is expected to produce a list, but getting %s. "%(key, data[key]), data)
    else:
        return data[key]



# ---------------------------- Page class and its parts ----------------------------


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

    @staticmethod
    def from_json(data:Dict[str,Any])->"ParBody":
        if optKey(data,"entity") is None:
            return ParBody(text=getKey(data,'text'))
        else:
            return ParBody(text=getKey(data,'text'), entity=getKey(data,'entity'), link_section=optKey(data,'link_section'), entity_name = optKey(data,'entity_name'))

class Paragraph(Jsonable):
    """
    Paragraph container that contains links / paragraph text. Is updated using ParagraphTextcollector class.
    """

    def __init__(self, para_id:str, para_body:Optional[List[ParBody]]=None)->None:
        self.para_id = para_id
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

    @staticmethod
    def from_json(data:Dict[str,Any])->"Paragraph":
        para_body = None
        if optKey(data,'para_body') is not None:
            para_body = [ParBody.from_json(d) for d in getListKey(data,'para_body')]

        return Paragraph(para_id=getKey(data,'para_id'), para_body=para_body)



class QueryFacet(Jsonable):
    """
    A query facet of a page (containing the facet's name and id)
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

    @staticmethod
    def from_json(data:Dict[str,Any])->"QueryFacet":
        return QueryFacet(facet_id=getKey(data, 'heading_id'), heading=getKey(data, 'heading'))

class ParagraphOrigin(Jsonable):
    """
    Contains information about the ranking from which a paragraph originates
    """
    def __init__(self, para_id:str, section_path:str, rank_score:float, rank:Optional[int])->None:
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
        if self.rank is not None:
            return self.__dict__
        else:
            tmp = self.__dict__.copy()
            tmp.__delitem__("rank")
            return tmp

    @staticmethod
    def from_json(data:Dict[str,Any])->"ParagraphOrigin":
        rank = None   # optional
        if 'rank' in data:
            rank = data['rank']
        return ParagraphOrigin(para_id=getKey(data, 'para_id'), section_path=getKey(data, 'section_path'), rank_score = data['rank_score'], rank = rank)


class Page(Jsonable):
    """
    A page that is populated

    """

    def __init__(self, squid: str, title: str, run_id: Optional[str], query_facets: List[QueryFacet]
                 # , facet_paragraphs: Optional[Dict[str, List[Paragraph]]] =  None    # None means None -- initialize with {} when needed
                 , paragraph_origins: Optional[List[ParagraphOrigin]] = None   # None means actually None here
                 , pids: Optional[Set[str]] = None                             # None means initialize with {}
                 , paragraphs: List[Paragraph] = None) -> None:                # None means initialize with []
        self.query_facets = query_facets  # type: List[QueryFacet]
        self.run_id = run_id  # set to None for page prototypes
        self.title = title
        self.squid = squid

        # paragraphs get loaded later
        self.pids = set() if pids is None else pids # type: Set[str]
        self.paragraphs = [] if paragraphs is None else paragraphs # type: List[Paragraph]

        # paragraph origins
        self.paragraph_origins = paragraph_origins # type: Optional[List[ParagraphOrigin]]



    def add_paragraph_origins(self, origin):
        if self.paragraph_origins is None:
            self.paragraph_origins = []
        self.paragraph_origins.append(origin)


    def copy_prototype(self,run_id:str)->"Page":
        return Page(squid = self.squid, title = self.title, run_id = run_id, query_facets = self.query_facets)


    def to_json(self, suppress_validation:bool = False):
        if not suppress_validation and not self.paragraphs:
            raise RuntimeError("Can only serialize populated pages to JSON, but page %s has no paragraphs." % self.squid)

        dictionary =  { "title": self.title
                , "squid": self.squid
                , "run_id": self.run_id
                , "paragraphs": [para.to_json() for  para in self.paragraphs]
                }
        if self.query_facets:
            dictionary["query_facets"] = [facet.to_json() for facet in self.query_facets]

        if self.paragraph_origins:
            dictionary["paragraph_origins"] = [origin.to_json() for origin in self.paragraph_origins]

        return dictionary

    @staticmethod
    def from_json(data:Dict[str,Any])->"Page":
        paragraphs = [Paragraph.from_json(d) for d in getListKey(data, 'paragraphs')]
        query_facets = [QueryFacet.from_json(d) for d in getListKey(data, 'query_facets')] if 'query_facets' in data else None
        paragraph_origins = [ParagraphOrigin.from_json(d) for d in getListKey(data, 'paragraph_origins')] if 'paragraph_origins' in data else None
        return Page(squid=getKey(data,'squid')
                    , title=getKey(data, 'title')
                    , run_id=optKey(data, 'run_id')
                    , query_facets = query_facets
                    , paragraphs = paragraphs
                    , paragraph_origins = paragraph_origins
                    , pids = {p.para_id for p in paragraphs}
                    )






    @staticmethod
    def fail_str(x):
        return not x or not isinstance(x, str) or len(x) == 0

    @staticmethod
    def fail_paragraph_id(x:str):
        return len(x) != 40 or [c for c in x if c not in "0123456789abcdef"]

    @staticmethod
    def fail_opt_int(x):
        if x is None:
            return False
        return not isinstance(x, int) or x < 0

    @staticmethod
    def fail_float(x):
        return x is None or not isinstance(x, float)


    def validate_minimal_spec(self)->List[ValidationError]:
        """ Minimal validation of loaded page and field types """

        errors = []
        def addValidationError( message:str):
            errors.append(ValidationError(message=message, data= self))

        
        if Page.fail_str(self.squid):
            addValidationError("Page squid %s (aka page id) of invalid type. Must be non-empty string."% self.squid)
        if Page.fail_str(self.run_id):
            addValidationError("Run id %s for page %s of invalid type. Must be non-empty string."% (self.run_id, self.squid))
        if not self.paragraphs:
            addValidationError("Paragraphs for page %s are empty. Must be non-empty list of paragraphs."% (self.squid))

        for paragraph in self.paragraphs:
            if not isinstance(paragraph, Paragraph):
                addValidationError("Paragraph in paragraphs of invalid type on page %s. Must be of type Paragraph."% (self.squid))
            if Page.fail_str(paragraph.para_id):
                addValidationError("Paragraph id %s in paragraphs for page %s of invalid type. Must be non-empty string."% (paragraph.para_id, self.squid))
            if Page.fail_paragraph_id(paragraph.para_id):
                addValidationError("Paragraph id %s in paragraphs for page %s of invalid type. Must contain 40 hexadecimal characters."% (paragraph.para_id, self.squid))

            if paragraph.para_body:
                if paragraph.para_body == []:
                    addValidationError("Paragraph id %s for page %s has empty para_body.  Must be either removed from JSON or non-empty list."% (paragraph.para_id, self.squid))

                for pbody in paragraph.para_body:
                    if Page.fail_str(pbody.text):
                        addValidationError("Paragraphs %s for page %s has invalid ParaBody. Paragraph bodies must contain a non-empty text field (alternatively paragraph bodies can be omitted)."% (paragraph.para_id, self.squid))


        for origin in self.paragraph_origins:
            if Page.fail_str(origin.para_id):
                addValidationError("Paragraph id %s in paragraph_origins of page %s of invalid type. Must be non-empty string."% (origin.para_id, self.squid))

            if Page.fail_paragraph_id(origin.para_id):
                addValidationError("Paragraph id %s in paragraph_origins of page %s of invalid type. Must contain 40 hexadecimal characters."% (origin.para_id, self.squid))

            if Page.fail_str(origin.section_path):
                addValidationError("Section path %s in paragraph_origins of page %s of invalid type. Must be non-empty string."% (origin.section_path, self.squid))

            if Page.fail_opt_int(origin.rank):
                addValidationError("Rank %d in paragraph_origins of page %s of invalid type. Must be non-negative integer or omitted."% (origin.rank, self.squid))

            if Page.fail_float(origin.rank_score):
                addValidationError("Rank score %f in paragraph_origins of page %s of invalid type. Must be float."% (origin.rank_score, self.squid))

        return errors

    def validate_required_y3_spec(self, top_k:int, maxlen_run_id:int)->List[Union[ValidationError, ValidationWarning]]:
        """ Validation of further constraints for Y3 submission, such as correct query name space (in squid) and page budget. """

        errors = []  # type: List[Union[ValidationError, ValidationWarning]]
        def addValidationError(message:str, is_warning:bool=False):
            if is_warning:
                errors.append(ValidationWarning(message=message, data= self))
            else:
                errors.append(ValidationError(message=message, data= self))

        if not self.squid.startswith("tqa2:"):
            addValidationError("Page squid %s (aka page id) is not in TREC CAR Y3 format. Must start with \'tqa2:\'." % self.squid)

        if "%20" in self.squid:
            addValidationError("Page squid %s (aka page id) is not in TREC CAR Y3 format. Must not contain \'%s\' symbols." % (self.squid, "%20"))

        if len(self.run_id)>maxlen_run_id:
            addValidationError("Run id %s is too long (%d). Must be max %d characters." % (self.run_id, len(self.run_id), maxlen_run_id))

        if len(self.paragraphs) > top_k:
            addValidationError("Page %s has too many paragraphs (%d); only top_k = %d paragraphs allowed per page." % (self.squid, len(self.paragraphs), top_k))

        if len(self.paragraphs) < top_k:
            addValidationError("Page %s has too few paragraphs (%d); page should contain top_k = %d paragraphs per page." % (self.squid, len(self.paragraphs), top_k), is_warning=True)
        return errors

    def validate_paragraph_y3_origins(self, top_k:int)->List[Union[ValidationError, ValidationWarning]]:
        """ Validation of paragraph origins, if provided.
        Paragraph origins are optional, but if given, they must be correct and consistent and using the right query name space (of squid).
        """

        errors = []  # type: List[Union[ValidationError, ValidationWarning]]
        def addValidationError(message:str, is_warning:bool=False):
            if is_warning:
                errors.append(ValidationWarning(message=message, data= self))
            else:
                errors.append(ValidationError(message=message, data= self))

        def pretty2(sort_by_score:List[ParagraphOrigin],sort_by_rank:List[ParagraphOrigin])->str:
            lines = ["%s\t%s"%(str(p1.to_json()), str(p2.to_json()))  for (p1,p2) in zip(sort_by_score, sort_by_rank)]
            return "sort_by_score\tsort_by_rank\n" + "\n".join(lines)

        def pretty(sort_by_rank:List[ParagraphOrigin])->str:
            lines = [str(p1.to_json()) for p1 in sort_by_rank]
            return "sort_by_rank\n" + "\n".join(lines)


        if not self.paragraph_origins:
            addValidationError("No paragraph origins defined for page %s"%self.squid, is_warning=True)

        for paragraph in self.paragraph_origins:
            if not paragraph.section_path.startswith("tqa2:"):
                addValidationError("Section path %s in is not in TREC CAR Y3 format. Must start with \'tqa2:\'." % paragraph.section_path)

            if "%20" in paragraph.section_path:
                addValidationError("Section path %s in is not in TREC CAR Y3 format.  is not in TREC CAR Y3 format. Must not contain \'%s\' symbols." % (paragraph.section_path, "%20"))

        found_section_paths = {p.section_path for p in self.paragraph_origins}
        required_section_paths = {qf.facet_id for qf in self.query_facets}
        for spath in found_section_paths - required_section_paths:
            addValidationError("Found section_path %s in paragraph_origins that does not belong for a section path of page %s. Must not be included. " % (spath, self.squid))

        for spath in required_section_paths - found_section_paths:
            addValidationError("Section_path %s of page %s not found in paragraph_origins. Rankings for all headings must be included. " % (spath, self.squid), is_warning= True)



        for (spath, paras) in safe_group_by((p.section_path,p) for p in self.paragraph_origins).items():
            if len(paras) > top_k:
                addValidationError("Paragraph_origins of section_path %s of page %s has %d entries, but must not include not than top_k=%d entries." % (spath, self.squid, len(paras), top_k))

            if len(paras) < top_k:
                addValidationError("Paragraph_origins of section_path %s of page %s has %d entries, but should include exactly top_k=%d entries." % (spath, self.squid, len(paras), top_k), is_warning=True)


        # Rank information is optional. If given perform these checks.
        if any(p.rank is not None for p in self.paragraph_origins):
            if( not all (p.rank is not None for p in self.paragraph_origins)):
                addValidationError("paragraph_origins for page %s have some entries, but not all entries. Must either be omitted or provided for all paragraph_origins."%(self.squid), is_warning= True)

            for p in self.paragraph_origins:
                if p.rank is not None and not p.rank >= 1:
                    addValidationError("Rank of paragraph_origins must be 1 or larger, but paragraph %s has rank %d on page %s. \n" %(p.para_id,p.rank, self.squid))


            for spath in found_section_paths:
                origins_for_spath = [p for p in self.paragraph_origins if p.section_path == spath]
                sort_by_score = sorted(origins_for_spath.copy(), key= lambda p: - p.rank_score)
                sort_by_rank = sorted(origins_for_spath.copy(), key= lambda p: -1 if p.rank is None else p.rank)

                skip_rest=False
                for (p1,p2) in zip(sort_by_score, sort_by_rank):
                    if (not skip_rest and (not p1.para_id == p2.para_id)):
                        addValidationError("Order of paragraph_origins by rank and by rank_score differ for "
                                           "paragraphs %s and %s for section_path %s on page %s. \n" %(p1.para_id,p2.para_id,spath, self.squid)
                                           + pretty2(sort_by_score,sort_by_rank))
                        skip_rest = True

                skip_rest=False
                last_rank = None
                for (p1,p2) in zip(sort_by_score, sort_by_rank):
                    if( not skip_rest and (last_rank is not None and p2.rank == last_rank)):
                        addValidationError("Same rank %d is used for multiple paragraph_origin "
                                           "section_path %s on page %s. \n" %(last_rank, spath, self.squid)
                                           + pretty(sort_by_rank))
                        skip_rest = True
                    last_rank = p2.rank


        return errors

def submission_to_json(pages: Iterator[Page]) -> str:
    return "\n".join([json.dumps(page.to_json()) for page in pages])

def json_to_pages(json_handle:TextIO)->Iterator[Page]:
    return (Page.from_json(json.loads(line)) for line in json_handle)



# ---------------------------- Run Parsing ----------------------------

class RunLine(object):
    """
    Object representing one line in a run file
    """

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

    def load_run_file(self,run_file, run_name: Optional[str]):
        with open(run_file) as f:
            for line in f:
                run_line = RunLine.from_line(line, run_name)
                if (run_line.rank <= self.top_k):
                    self.runlines.append(run_line)



class RunPageKey(object):
    """
    Hashable key of (run_name, squid) pairs.
    """
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
