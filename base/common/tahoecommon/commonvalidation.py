from deepdiff import DeepDiff
import boto3
from pyspark.sql.dataframe import *
from tahoecommon.commonglue import *
from typing import Dict

class BasicSchemaValidator():
    def __init__(self, frame, glue_database_name: str, glue_table_name: str):
        '''
        :param frame: frame to validate
        :param differences: differences see deepdiff return values for more info
        :param glue_database_name: database name to validate against
        :param glue_table_name: table name to validate against
        '''
        self.frame = frame
        self.differences:DeepDiff = None
        self.glue_database_name = glue_database_name
        self.glue_table_name = glue_table_name

    def compare_schema(self, exclude_callback=None):  
        '''
        Compares the current frame to glue table

        :param exclude_callback: excludes certain columns from deepdiff
        '''
        col_mappings: Dict[str, str] = self.get_current_glue_table_columns()

        if col_mappings is None:
            return
        # check current s3 location if validation failed 

        x = {k.lower(): self.convert_complex_spark(v) for k, v in self.frame.dtypes}
        y = {k.lower(): self.convert_complex_spark(v) for k, v in col_mappings.items()}           

        print("NEW SCHEMA")        
        print(x)
        print("CURRENT SCHEMA")
        print(y)
                                
        differences = DeepDiff(y, x, ignore_order=True, exclude_obj_callback=exclude_callback)
        
        print("DIFFERENCES", str(differences))
        if len(differences) != 0:
            self.differences = differences

    def __str__(self) -> str:
        '''
        pretty prints string output for error messages (TODO)

        :returns: pretty string 
        '''
        if self.differences:
            return self.differences.pretty()
        return "No changes found"
        
    def get_current_glue_table_columns(self) -> dict:
        '''
        Get current table schema

        :returns: Glue column name to table types
        '''
     
        old_table_schema = get_table(self.glue_database_name, self.glue_table_name)
     
        if old_table_schema == None:
            return None
        col_mappings = {}
        for col in old_table_schema["Table"]["StorageDescriptor"]["Columns"]:
            col_mappings[col["Name"]] = col["Type"]
        return col_mappings

    def convert_complex_spark(self, typing_info: str):
        '''
        Converts spark dataframe types into python object that can be compared

        :param typing_info: spark dataframe schema
        '''
        # check if complex type
        if "array<" in typing_info or "struct<" in typing_info:
            tracking_sign = [(-1, "", "")]
            # find all signs and create a list of tuples with info: (index[0], symbol[1], type/key/complex type[2])
            for i,x in enumerate(typing_info):
                if x == "<" or x == ">" or x == "," or x == ":":
                    # Null types default to string in the glue tables
                    if typing_info[tracking_sign[-1][0]+1 :i] == "null":
                        type = "string"
                    else:
                        type = typing_info[tracking_sign[-1][0]+1 :i]
                    tracking_sign.append((i,x, type))
            data_types = []
                
            # drop first since not required for translation
            tracking_sign = tracking_sign[1:]
            # Turn complex types into python readable
            for i, x in enumerate(tracking_sign):
                # signifies the start of an array complex type
                if x[1] == '<' and x[2] == 'array':
                    # need to check if array originates from struct so we know where to place it back into
                    from_key = tracking_sign[i-1][2] if i > 0 and tracking_sign[i-1][1] == ":" else None
                    # add it to the complex tracker to signify next values will eventually merge into this construct
                    data_types.append(([],from_key))
                # signifies the struct complex type
                if x[1] == '<' and x[2] == 'struct':
                    from_key = tracking_sign[i-1][2] if i > 0 and tracking_sign[i-1][1] == ":" else None
                    data_types.append(({}, from_key))
                # a place holder value for key used for debug if value is None parse failed
                if x[1] == ':':
                    data_types[-1][0][x[2]] = None
                # place value using previous tracking_sign value which was a key
                if x[1] == ',' and x[2] != '':
                    data_types[-1][0][tracking_sign[i-1][2]] = x[2]
                    
                # the data_type has ended so add final value or complete if small array 
                if x[1] == '>' and (x[2] != "" or len(data_types) ==1):
                    if isinstance(data_types[-1][0], list):
                        data_types[-1][0].append(x[2])
                    if isinstance(data_types[-1][0], dict):
                        data_types[-1][0][tracking_sign[i-1][2]] = x[2]
                    if len(data_types) == 1:
                        return data_types[0][0]
                # Merge data type into previous data type (nested)     
                if x[2] == "":                    
                    instance = data_types.pop()
                    if instance[1] == None:
                        data_types[-1][0].append(instance[0])
                    if instance[1] != None:
                        data_types[-1][0][instance[1]] = instance[0]
                    if len(data_types) == 1:
                        return data_types[0][0]
        else:
            # not complex so keep type
            return typing_info