import bz2
import gzip
import lzma
from typing import TextIO, Iterator, Tuple, Any, Dict, List


def maybe_compressed_open(loc:str, mode:str='rt')->TextIO:
    """
    Open file wit UTF-8, which may be compressed with gz, xz, bz2 or uncompressed.
    Default mode is 'rt', can be overwritten.
    """
    if loc.endswith(".gz"):
        return gzip.open(loc, mode=mode, encoding='utf-8')
    elif loc.endswith(".xz"):
        return lzma.open(loc, mode=mode, encoding='utf-8')
    elif loc.endswith(".bz2"):
        return bz2.open(loc, mode=mode, encoding='utf-8')
    else:
        return open(loc,'rt', encoding='utf-8')



def safe_group_by(pairs:Iterator[Tuple[str,Any]])->Dict[str,List[Any]]:
    """
        performs a group_by on an unsorted list of key-value pairs. Values of duplicate keys will be accumulated into list.
        :param pairs: list of (key, value)
    """

    res = {} # type: Dict[str,List[Any]]
    for (k,v) in pairs:
        if k  not in res:
            res[k] = []
        res[k].append(v)
    return res

def safe_group_list_by(pairs:Iterator[Tuple[str,List[Any]]])->Dict[str,List[Any]]:
    """
        performs a group_by on an unsorted list of key-list-value pairs. Valued of duplicate keys will be accumulated in concatenated lists
        :param pairs: list of (key, list of values)
    """
    res = {} # type: Dict[str,List[Any]]
    for (k,lst) in pairs:
        if k  not in res:
            res[k] = []

        res[k].extend(lst)
    return res

