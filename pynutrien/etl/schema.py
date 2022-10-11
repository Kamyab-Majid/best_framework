class Schema:
    pass


class NameSchema(Schema):
    id: int
    first_name: str
    last_name: str


class FullNameSchema(NameSchema):
    full_name: str


def combine_first_last(df: NameSchema) -> FullNameSchema:
    pass
