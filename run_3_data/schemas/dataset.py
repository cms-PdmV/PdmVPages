"""
This module models the schema required
for RAW datasets and for matching its sublevel (children) data tiers.
"""
from typing import List, Optional
import copy
import pprint
import re

class Stats2Information:
    """
    Data object to store Stats2 information
    for some output datasets.

    Attributes:
        dataset (str): Dataset information to retrieve.
            using the `output_dataset` parameter.
        campaigns (list[str]): List of campaigns the dataset is related
            according to Stats2 records.
        input_dataset (str): Parent dataset used to produce this one.
        workflow (str): ReqMgr2 request that produced this dataset.
        prepid (str): PdmV identifier related to the request that produced
            this dataset. For this context, it is the identifier for the ReReco request
            in PdmV ReReco application.
        processing_string (str): Dataset's processing string.
        raw (dict): Stats2 record for this dataset.
    """
    def __init__(self, dataset: str, raw: dict) -> None:
        self.dataset: str = dataset
        self.raw: dict = raw
        self.campaigns: List[str] = self.raw.get("Campaigns", [])
        self.input_dataset: str = self.raw.get("InputDataset", "")
        self.workflow: str = self.raw.get("RequestName", "")
        self.prepid: str = self.raw.get("PrepID", "")
        self.processing_string: str = self.raw.get("ProcessingString", "")


class DatasetMetadata:
    """
    Models all the attributes that could be extracted from
    the dataset name and determines if the given
    dataset name is valid using a given regex.

    Attributes:
        full_name (str): Original dataset name, e.g: /BTagMu/Run2023C-PromptReco-v4/AOD
        primary_dataset (str): Dataset primary dataset, e.g: BTagMu
        era (str): Dataset era, e.g: Run2023C
        year (str): Year related to the era, e.g: 2023
        processing_string (str): Dataset's processing string, e.g: PromptReco
        filtered_ps (str): In case the processing string includes a version,
            this field stores the PS without this attribute, e.g: 
            - /BTagMu/Run2022G-PromptNanoAODv11_v1-v2/NANOAOD
                processing_string := PromptNanoAODv11_v1
                filtered_ps := PromptNanoAODv11
        version (str): Dataset's version, e.g: v4
        datatier (str): Dataset's data tier, e.g: AOD
        valid (bool): Determines if the dataset is valid using a predefined regex.
    """
    ATTRIBUTE_REGEX: str = r"^/(\w*)/(Run[0-9]{4}[A-Z]){1}-(\w*)-(v[0-9]{1,2})/([A-Z]*)$"
    SUBVERSION_REGEX: str = r"^(\w*)_(v[0-9]{1,2})$"
    RAW_REGEX: str = r"^/(\w*)/(Run[0-9]{4}[A-Z]){1}-(v[0-9]{1,2})/(RAW)$"
    RAW = re.compile(RAW_REGEX)
    VALIDATOR = re.compile(ATTRIBUTE_REGEX)
    SUBVERSION = re.compile(SUBVERSION_REGEX)

    def __init__(self, name: str) -> None:
        self.full_name: str = name
        self.primary_dataset: str = ""
        self.era: str = ""
        self.processing_string: str = ""
        self.filtered_ps: str = ""
        self.version: str = ""
        self.datatier: str = ""
        self.__valid: bool = False

        # Update the attributes
        self.__build()

    def __build(self) -> None:
        """
        Parses the metadata
        """
        is_raw: bool = self.__build_raw()
        if not is_raw:
            self.__build_non_raw()

    def __build_raw(self) -> bool:
        """
        Checks if the dataset is related to a RAW one
        and parses its data
        """
        components: List[str] = DatasetMetadata.RAW.findall(self.full_name)
        if not components:
            return False
        
        # Parse the data
        primary_ds, era, version, datatier = components[0]
        self.primary_dataset = primary_ds
        self.era = era
        self.year = era[3:-1]
        self.version = version
        self.datatier = datatier
        self.__valid = True
        return True

    def __build_non_raw(self) -> None:
        """
        Parse the metadata for a non RAW dataset
        """
        components: List[str] = DatasetMetadata.VALIDATOR.findall(self.full_name)
        if not components:
            return
    
        # Parse the fields
        primary_ds, era, ps, version, datatier = components[0]
        self.primary_dataset = primary_ds
        self.era = era
        self.year = era[3:-1]
        self.processing_string = ps
        self.version = version
        self.datatier = datatier
        self.__valid = True

        # Check if the version is overwritten in the PS
        self.__check_ps()

    def __check_ps(self) -> None:
        """
        Check the processing string to determine
        if there is a version tag that overwrites the
        current one.
        """
        components: List[str] = DatasetMetadata.SUBVERSION.findall(self.processing_string)
        if not components:
            return
        
        # Parse the fields
        filtered_ps, version = components[0]
        self.filtered_ps = filtered_ps
        self.version = version

    @property
    def valid(self) -> bool:
        return self.__valid
    
    def __repr__(self) -> str:
        repr: str = (
            f"Dataset <valid={self.valid} name={self.full_name} "
            f"primary_dataset={self.primary_dataset} era={self.era} "
            f"processing_string={self.processing_string} version={self.version} "
            f"datatier={self.datatier}>"
        )
        return repr


class ChildDataset:
    """
    This class represents the child datasets derived from a RAW dataset.
    They could be datasets for AOD, MiniAOD and NanoAOD datatiers.

    Attributes:
        metadata (DatasetMetadata): Models the dataset name following DBS conventions
        events (int): Number of events registered for the dataset
        runs: (list[int]): Number of runs linked to the dataset when it was created
        type (str): Dataset type registered into DBS. For example: VALID, INVALID, PRODUCTION
        campaign (str): Name of the PdmV campaign linked to this dataset.
            For instance, if you check JSON control file (data/years.json)
            some attributes linked to this are: MiniAODv3 or NanoAODv11
        output (List[ChildDataset]): List of child datasets. For example, if this object represents
            a AOD dataset, this field will contain the MiniAOD and NanoAOD if they exist.
        prepid (str): Name of the prepid entry for this dataset into Stats2. This is only modeled
            as a compatibility mode due to we are taking all this information directly from
            DBS
        workflow (str): Link to the computing workflow related to the production of this dataset
            This is only modeled for a compatibility mode.
    """

    def __init__(
        self,
        metadata: DatasetMetadata,
        events: int = -1,
        runs: Optional[List[int]] = None,
        type: str = "",
        campaign: str = "",
        output: Optional[List] = None,
        prepid: Optional[str] = None,
        workflow: Optional[str] = None,
    ):
        self.metadata = metadata
        self.events = events
        self.runs = [] if not runs else runs
        self.type = type
        self.campaign = campaign
        self.output = [] if not output else output
        self.prepid = prepid
        self.workflow = workflow

    @property
    def dict(self) -> dict:
        self.output = sorted(self.output, key=lambda cd: cd.metadata.full_name)
        child_dataset: List[dict] = [cd.dict for cd in self.output]

        return {
            "dataset": self.metadata.full_name,
            "events": self.events,
            "runs": self.runs,
            "type": self.type,
            "campaign": self.campaign,
            "processing_string": self.metadata.processing_string,
            "datatier": self.metadata.datatier,
            "era": self.metadata.era,
            "prepid": self.prepid,
            "workflow": self.workflow,
            "output": child_dataset,
        }

    def __repr__(self) -> str:
        repr: str = (
            f"<ChildDataset full_name={self.metadata.full_name} "
            f"children={len(self.output)} output_id={id(self.output)}>"
        )
        return repr

class RAWDataset:
    """
    Models the attributes required for representing
    a RAW dataset into the original schema and also acts as a data object.

    Attributes:
        dataset (str): Models the dataset name following DBS conventions
        events (int): Number of events registered for the dataset
        twiki_runs: (list[int]): Number of validated runs linked to the dataset.
            This attribute is going to be included after Run 3 finishes. It is model
            here as a compatibility mode for Run 2 UL page
        year: (int): Year when the dataset was produced
        runs: (list[int]): Number of runs linked to the dataset when it was created
        output (list[ChildDataset]): List of child datasets for AOD, MiniAOD and NanoAOD
            datatiers linked to this RAW dataset.
    """

    def __init__(
        self,
        dataset: str,
        events: int,
        year: int,
        runs: List[int],
        output: Optional[List[ChildDataset]] = None,
        twiki_runs: Optional[List[int]] = None,
    ):
        self.dataset = dataset
        self.events = events
        self.year = year
        self.runs = runs
        self.output = [] if not output else output
        self.twiki_runs = [] if not twiki_runs else twiki_runs

    @property
    def dict(self) -> dict:
        self.output = sorted(self.output, key=lambda cd: cd.metadata.full_name)
        child_dataset: List[dict] = [cd.dict for cd in self.output]

        return {
            "dataset": self.dataset,
            "events": self.events,
            "year": self.year,
            "runs": self.runs,
            "output": child_dataset,
            "twiki_runs": self.twiki_runs,
        }

    def __repr__(self) -> str:
        return pprint.pformat(
            self.dict,
            width=100,
            compact=True,
            depth=100
        )


class PageMetadata:
    """
    Models and stores the campaign matching given in the 
    input file: 'data/years.json'. This files defines the
    PdmV campaign tag for a specific group of processing strings
    based on the data tier and the era.

    Attributes:
        metadata (dict): Original content of the years file.
        transformed (dict): Metadata content parsed to easily
            retrieve the campaign.
    """
    def __init__(self, metadata: dict) -> None:
        self.metadata: dict = metadata
        self.transformed: dict = self.__parsed_content()

    def __parsed_content(self) -> dict:
        """
        Iterates over all the dictionary and reduce the
        campaigns and processing string per data tier to
        a direct match, e.g: 

        "MINIAOD": [
          {
            "campaign": "MiniAODv3",
            "processing_string": ["27Jun2023"]
          }
        ],

        to:

        "MINIAOD": {
          "27Jun2023": "MiniAODv3",
          "14Apr2023": "MiniAODv3"
        },

        Returns:
            A dictionary with the processing string and campaigns
            flatten to query them directly.
        """
        transformed: dict = copy.deepcopy(self.metadata)

        for year, data in self.metadata.items():
            era_content: dict = data.get("era", {})
            for run, data_tier_content in era_content.items():
                for data_tier, campaign_content in data_tier_content.items():
                    reduced_ps: dict = {}
                    for campaign_match in campaign_content:
                        campaign: str = campaign_match["campaign"]
                        processing_str: List[str] = campaign_match["processing_string"]
                        campaign_tags: dict = {
                            ps: campaign
                            for ps in processing_str
                        }
                        reduced_ps = {**reduced_ps, **campaign_tags}
                    
                    # Update the transformed data
                    transformed[year]["era"][run][data_tier] = reduced_ps
        
        return transformed
    
    def campaign(self, metadata: DatasetMetadata) -> str:
        """
        Retrieves the campaign related to the data set
        processing string. In case the campaign tag is not
        available, the tag '<other>' will be returned.

        Args:
            metadata (DatasetMetadata): Data set metadata.
        """
        ps: str = metadata.filtered_ps or metadata.processing_string
        try:
            campaign: str = (
                self.transformed[metadata.year]
                ["era"][metadata.era]
                [metadata.datatier][ps]
            )
            return campaign
        except KeyError:
            return "<other>"
