from functools import partial
from typing import Any, Dict, List, Tuple

from pandas import DataFrame
from tqdm.contrib import concurrent

from lib.pipeline import DefaultPipeline
from lib.wikidata import wikidata_properties


class WikidataPipeline(DefaultPipeline):
    """ Retrieves the requested properties from Wikidata for all items in metadata.csv """

    @staticmethod
    def _process_item(props: Dict[str, str], key_wikidata: Tuple[str, str]):
        key, wikidata_id = key_wikidata
        return {"key": key, **wikidata_properties(props, wikidata_id)}

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts):
        props: Dict[str, str] = parse_opts["properties"]
        data = aux["knowledge_graph"].merge(aux["metadata"])[["key", "wikidata"]].set_index("key")

        # Load wikidata using parallel processing
        map_iter = data.wikidata.iteritems()
        map_func = partial(WikidataPipeline._process_item, props)
        records = concurrent.thread_map(map_func, map_iter, total=len(data))

        # Return all records in DataFrame form
        return DataFrame.from_records(records)
