
from typing import Dict, List
from stacks.shared.constructs.tahoe_datasource_nested import TahoeDatasourceNestedStack
from stacks.shared.constructs.tahoe_construct import TahoeConstruct


class DependentDatasources():
    def __init__(self, *datasources: TahoeDatasourceNestedStack):
        self.queried: Dict[str, TahoeDatasourceNestedStack] = {}
        self.stored: Dict[str, TahoeDatasourceNestedStack] = {}
        self.datasources = datasources
        self.deploy = True

        for datasource in datasources:
            if datasource is None:
                self.deploy = False
                break
            if datasource.is_queryable:
                self.queried[datasource.data_prefix] = datasource
            else:
                self.stored[datasource.data_prefix] = datasource

    def get_datasource(self, name):
        if name in self.queried:
            return self.queried[name]
        if name in self.stored:
            return self.stored[name]

    def deployable(self):
        return self.deploy
