from flasgger import Schema, fields

class CloudformationResource(Schema):
    name = fields.Str()
    type = fields.Str()
    id = fields.Str()
    last_update = fields.Str()
