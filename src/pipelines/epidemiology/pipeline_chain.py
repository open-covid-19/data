from typing import Any, Dict, List, Tuple

from pandas import DataFrame, Series, Int64Dtype

from lib.time import date_offset
from lib.pipeline import DataPipeline, PipelineChain

from .au_covid_19_au import Covid19AuPipeline
from .br_covid19_brazil_timeseries import Covid19BrazilTimeseriesPipeline
from .ca_authority import CanadaPipeline
from .ch_openzh import OpenZHPipeline
from .co_authority import ColombiaPipeline
from .de_covid_19_germany_gae import Covid19GermanyPipeline
from .es_authority import ISCIIIPipeline
from .es_datadista import DatadistaPipeline
from .fr_france_covid_19 import FranceCovid19Pipeline
from .gb_covid_19_uk_data import Covid19UkDataL2Pipeline, Covid19UkDataL3Pipeline
from .id_catchmeup import CatchmeupPipeline
from .it_authority import PcmDpcL1Pipeline, PcmDpcL2Pipeline
from .jp_2019_ncov_japan import Jp2019NcovJapanByDate
from .mx_mexico_covid_19 import MexicoCovid19Pipeline
from .nl_corona_watch_nl import CoronaWatchNlPipeline
from .pt_covid19pt import Covid19PtPipeline
from .si_authority import SloveniaPipeline
from .us_covidtracking import CovidTrackingPipeline
from .us_nyt_covid import NytCovidL2Pipeline, NytCovidL3Pipeline
from .xx_covid19_eu_data import Covid19EuDataPipeline
from .xx_dxy import DXYPipeline
from .xx_ecdc import ECDCPipeline
from .xx_open_covid_19 import OpenCovid19Pipeline
from .xx_owid import OurWorldInDataPipeline
from .xx_wikipedia import WikipediaPipeline

_wiki_base_url: str = "https://en.wikipedia.org/wiki"
_wiki_template_path: str = "Template:2019–20_coronavirus_pandemic_data"


class EpidemiologyPipelineChain(PipelineChain):

    schema: Dict[str, type] = {
        "date": str,
        "key": str,
        "new_confirmed": Int64Dtype(),
        "new_deceased": Int64Dtype(),
        "new_recovered": Int64Dtype(),
        "new_tested": Int64Dtype(),
        "total_confirmed": Int64Dtype(),
        "total_deceased": Int64Dtype(),
        "total_recovered": Int64Dtype(),
        "total_tested": Int64Dtype(),
    }

    pipelines: List[Tuple[DataPipeline, Dict[str, Any]]] = [
        # Start with yesterday's data to make sure that we carry over datapoints in case the data
        # source has gone offline or is temporarily unavailable
        # (OpenCovid19Pipeline(), {}),
        # Data sources for all countries level 1
        (OurWorldInDataPipeline(), {}),
        (ECDCPipeline(), {}),
        # Data sources for AR level 2
        (
            WikipediaPipeline(
                "{}/{}/Argentina_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {
                "parse_opts": {
                    "date_format": "%d %b",
                    "country": "AR",
                    "skiprows": 1,
                    "cumsum": True,
                }
            },
        ),
        # Data sources for AT level 2
        (
            Covid19EuDataPipeline("AT"),
            # Remove dates with known bad data
            # TODO: apply patch to make up for missing dates
            {"filter_func": lambda x: not x.date in ["2020-04-14", "2020-04-15"]},
        ),
        # Data sources for AU level 2
        (Covid19AuPipeline(), {}),
        (
            WikipediaPipeline(
                "{}/{}/Australia_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {"parse_opts": {"date_format": "%d %B", "country": "AU", "cumsum": True}},
        ),
        # Data sources for BO level 2
        (
            WikipediaPipeline(
                "{}/{}/Bolivia_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {
                "parse_opts": {
                    "date_format": "%b %d",
                    "country": "BO",
                    "skiprows": 1,
                    "droprows": "Date(2020)",
                }
            },
        ),
        # Data sources for BR level 2
        (Covid19BrazilTimeseriesPipeline(), {}),
        # Data sources for CA level 2
        (CanadaPipeline(), {}),
        # Data sources for CH level 2
        (OpenZHPipeline(), {}),
        # Data sources for CL level 2
        (
            WikipediaPipeline(
                "{}/{}/Chile_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {"parse_opts": {"date_format": "%Y-%m-%d", "country": "CL", "skiprows": 1}},
        ),
        # Data sources for CN level 2
        (DXYPipeline(), {"parse_opts": {"country_name": "China"}}),
        # Data sources for CO levels 2 + 3
        (ColombiaPipeline(), {}),
        # Data sources for CZ level 2
        (Covid19EuDataPipeline("CZ"), {}),
        # Data sources for DE level 2
        (Covid19GermanyPipeline(), {}),
        # Data sources for ES levels 1 + 2
        # (DatadistaPipeline(), {}),
        (ISCIIIPipeline(), {}),
        # Data sources for FR level 2
        (
            WikipediaPipeline(
                "{}/{}/France_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {"parse_opts": {"date_format": "%Y-%m-%d", "country": "FR", "skiprows": 1}},
        ),
        (FranceCovid19Pipeline(), {}),
        # Data sources for GB lebels 2 + 3
        (Covid19UkDataL2Pipeline(), {}),
        (Covid19UkDataL3Pipeline(), {}),
        # Data sources for ID level 2
        (CatchmeupPipeline(), {}),
        # Data sources for IN level 2
        (
            WikipediaPipeline("{}/2020_coronavirus_pandemic_in_India".format(_wiki_base_url)),
            {"parse_opts": {"date_format": "%b-%d", "country": "IN", "skiprows": 1}},
        ),
        # Data sources for IT level 2
        (PcmDpcL1Pipeline(), {}),
        (PcmDpcL2Pipeline(), {}),
        # Data sources for JP level 2
        (
            WikipediaPipeline(
                "{}/{}/Japan_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {"parse_opts": {"date_format": "%Y/%m/%d", "country": "JP", "skiprows": 2}},
        ),
        (Jp2019NcovJapanByDate(), {}),
        # Data sources for KR level 2
        (
            WikipediaPipeline(
                "{}/{}/South_Korea_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {"parse_opts": {"date_format": "%Y-%m-%d", "country": "KR", "skiprows": 1}},
        ),
        # Data sources for MY level 2
        (
            WikipediaPipeline("{}/2020_coronavirus_pandemic_in_Malaysia".format(_wiki_base_url)),
            {
                "parse_opts": {
                    "date_format": "%d/%m",
                    "country": "MY",
                    "cumsum": True,
                    "drop_column": "deceased",
                }
            },
        ),
        # Data sources for MX level 2
        (MexicoCovid19Pipeline(), {}),
        # Data sources for NL levels 2 + 3
        (CoronaWatchNlPipeline(), {}),
        # Data sources for NO level 2
        (Covid19EuDataPipeline("NO"), {}),
        # Data sources for PE level 2
        (
            WikipediaPipeline(
                "https://es.wikipedia.org/wiki/Pandemia_de_enfermedad_por_coronavirus_de_2020_en_Per%C3%BA"
            ),
            {
                "parse_opts": {
                    "date_format": "%d de %B",
                    "country": "PE",
                    "locale": "es_ES",
                    "skiprows": 1,
                }
            },
        ),
        # Data sources for PK level 2
        (
            WikipediaPipeline(
                "{}/{}/Pakistan_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {
                "parse_opts": {
                    "date_format": "%b %d",
                    "country": "PK",
                    "skiprows": 1,
                    "cumsum": True,
                }
            },
        ),
        # Data sources for PL level 2
        (Covid19EuDataPipeline("PL"), {}),
        # Data sources for PT level 2
        (Covid19PtPipeline(), {}),
        # Data sources for RU level 2
        (
            WikipediaPipeline(
                "{}/{}/Russia_medical_cases".format(_wiki_base_url, _wiki_template_path)
            ),
            {"parse_opts": {"date_format": "%d %b", "country": "RU", "skiprows": 1}},
        ),
        # Data sources for SE level 2
        (Covid19EuDataPipeline("SE"), {}),
        # Data sources for SI level 1
        (SloveniaPipeline(), {}),
        # Data sources for US levels 2 + 3
        (CovidTrackingPipeline(), {}),
        (NytCovidL2Pipeline(), {}),
        (NytCovidL3Pipeline(), {}),
    ]
