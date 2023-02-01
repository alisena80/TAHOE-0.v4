from typing import List, Mapping
from stacks.shared.constructs.tahoe_construct import TahoeConstruct
from stacks.shared.lib.base_config import BaseConfig
import os
import glob

from stacks.shared.lib.build_config import BuildConfig
from stacks.shared.lib.enumerations import GlueVersions


class TahoePipelineConstruct(TahoeConstruct):
    """
    A class to represent a basic tahoe datasource/consumer agnostic source

    Attributes
    ----------
    data_prefix  : str
        data prefix for datasource/consumer
    base_context: BaseConfig
        Contains all base stack info that needs to be exposed
    function: l.Lambda
        trigger function
    rule: events.Rule
        trigger timer 
    """
    def __init__(self, scope: TahoeConstruct, id: str, build_context: BuildConfig, base_context: BaseConfig, data_prefix, sub_path=None):
        """
        Represents datasource and consumer constructs

        Parameters
        ----------
            scope : Construct
                construct scope
            construct_id : str
                construct id
            build_context : BuildConfig
                build configuration settings
            data_prefix: str
                data prefix to name/describe
            base_context: BaseConfig
                Contains all base stack info that needs to be exposed
            sub_path: str
                Absolute path to consumer or datasource


        """
        super().__init__(scope, id, build_context)
        self.data_prefix: str = data_prefix
        self.base_context: BaseConfig = base_context
        self.sub_path: str = sub_path
        self.create_logger()
        

    def parameterize(self, params: Mapping[str, str], python_files: List[str], python_modules: List[str], jars: List[str], has_network: bool, version: GlueVersions):
        """
        Converts parameters to glue readable parameters

        Parameters
        ----------
            params: Mapping[str:str]
                root path to find file that will start the step function
            python_files: List[str]
                python files zip/whls
            python_modules : List[str]
                python modules pip modules
            jars: List[str]
                spark jars used to add additional functionality to glue spark jars
            has_network: bool
                check for connection to enable wheelhouse
            version: string
                python version
        """
        args = {}
        self.logger.debug(f"params: {params}")
        self.logger.debug(f"jars: {jars}")
        self.logger.debug(f"python_files: {python_files}")
        self.logger.debug(f"python_modules: {python_modules}")
        for x in params:
            if params[x] is not None and "$." in params[x]:
                args["--" + x + ".$"] = params[x]
            else:
                args["--" + x] = params[x]
        
        if len(python_modules) > 0:
            python_modules = self.attach_validated_version(python_modules, version)
            # download module from internet
            args["--additional-python-modules"] = ",".join(python_modules)
            # Only check wheelhouse if glue is connected with vpc
            if has_network:
                args["--python-modules-installer-option"] = "--no-index --find-links {} --trusted-host {}"\
                    .format(self.base_context.get_wheelhouse_bucket().bucket_website_url, self.base_context.get_wheelhouse_bucket().bucket_website_domain_name)
        if len(python_files) > 0:
            self.validate_py_files(python_files)
            args["--extra-py-files"] = ",".join(python_files)
        if jars:
            self.validate_jars(jars)
            args["--extra-jars"] = ",".join(jars)
        self.logger.debug(f"Glue readable args: {args}")
        return args

    def attach_validated_version(self, modules: List[str], version: GlueVersions):
        """
        Validates py modules to ensure the modules exist in base/wheelhouse/(version)requirements.txt file. If exists attaches appropriate version number.

        Parameters
        ----------
            py_modules: List[str]
                pip modules to install in glue
            version:
                whether the job is a pyshell or pyspark job
        """
        module_list = []
        error_modules = []
        for module in modules:
            for module_with_version in self.build_context.get_etl_python_modules(version):
                if module_with_version.startswith(module):
                    module_list.append(module_with_version)
                    break
            else:
                error_modules.append(module)
        if len(error_modules) > 0:
            raise ValueError("The module/s is missing " + str(error_modules))
        return module_list

    def validate_py_files(self, py_files:List[str]):
        """
        Validates py files to ensure the type is whl or zip and is in base folder

        Parameters
        ----------
            py_files: List[str]
                files to verify

        """
        for x in py_files:
            if os.path.splitext(x)[-1] != ".whl" and os.path.splitext(x)[-1] != ".zip":
                raise ValueError("The file " + x +  " is not a whl or zip")
            self.find_file(x.split("/")[-1], "base")

    def validate_jars(self, jars: List[str]):
        """
        Validates jar file type and in base folder

        Parameters
        ----------
            jars: List[str]
                jars to use in glue
        """
        for x in jars:
            if os.path.splitext(x)[-1] != ".jar":
                raise ValueError("The file " + x + " is not a jar")
            self.find_file(x.split("/")[-1], "base")
    
    def find_file(self, file : str, path : str):
        """
        Checks for file in a folder

        Parameters
        ----------
            file: str
                file name to search for
            path: str
                path to search in
        """
        os.chdir("../" + path)
        return len(glob.glob(os.getcwd() + '/**/' + file, recursive=True)) > 0

    def name_from_sub_path(self) -> str:
        """
        Returns end of folder path to distinguish between datasource or consumer
        """
        return self.sub_path.split("/")[-1]
