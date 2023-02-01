from flasgger import Schema, fields

class NvdConfigurationSchema(Schema):
    cve_data_version = fields.Str()
    nodes = fields.Int()

class CVEMetaSchema(Schema):
    id = fields.Str()
    assigner = fields.Str()

class CVESchema(Schema):
    data_type = fields.Str()
    data_format = fields.Str()
    data_version = fields.String()
    cve_data_meta = fields.Nested(CVEMetaSchema)
    problem_type = fields.Nested({"problemtype_data": fields.Int()})
    references = fields.Nested({"reference_data": fields.Int()})
    description = fields.Nested({"description_data": fields.Int()})

class BaseMetricV2Cvssv2Schema(Schema):
    version = fields.Str()
    vectorstring = fields.Str()
    accessvector = fields.Str()
    accesscomplexity = fields.Str()
    authentication = fields.Str()
    confidentialityimpact = fields.Str()
    integrityimpact = fields.Str()
    availabilityimpact = fields.Str()
    basescore = fields.Decimal()

class BaseMetricV3Cvssv3Schema(Schema):
    version = fields.Str()
    vectorstring = fields.Str()
    accessvector = fields.Str() 
    attackcomplexity = fields.Str()
    privilegesrequired = fields.Str()
    userinteraction = fields.Str()
    scope = fields.Str()
    confidentialityimpact = fields.Str()
    integrityimpact = fields.Str()
    availabilityimpact = fields.Str()
    basescore = fields.Decimal()
    baseseverity = fields.Str()

class BaseMetricv2Schema(Schema):
    cssv2 = fields.Nested(BaseMetricV2Cvssv2Schema)
    severity = fields.Str()
    exploitabilityscore = fields.Int()
    impactscore = fields.Decimal()
    obtainallprivilege = fields.Decimal()
    obtainuserprivilege = fields.Bool()
    obtainotherprivilege = fields.Bool()
    userinteractionrequired = fields.Bool()
    acinsufinfo = fields.Bool()

class BaseMetricv3Schema(Schema):
    cssv3 = fields.Nested(BaseMetricV3Cvssv3Schema)
    exploitabilityscore = fields.Decimal()
    impactscore = fields.Decimal()

class ImpactSchema(Schema):
    basemetricv2 = fields.Nested(BaseMetricv2Schema)
    basemetricv3 = fields.Nested(BaseMetricv3Schema)

class NvdSchema(Schema):
    cve = fields.Nested(CVESchema)
    configurations = fields.Nested(NvdConfigurationSchema)
    impact = fields.Nested(ImpactSchema)
    publisheddate = fields.Str()
    lastmodifieddate = fields.Str()

class NvdQueryBodySchema(Schema):
    publisheddate = fields.Str()
    attack_vector = fields.Str()

class NvdDescriptionListSchema(Schema):
    items = fields.List(fields.Str())

class NvdDescriptionQueryBodySchema(Schema):
    keyword = fields.Str()