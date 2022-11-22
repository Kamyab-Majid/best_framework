from __future__ import annotations

import boto3
import awswrangler as wr


class DynamoDB:
    def __init__(self, session):
        self.session: boto3.session.Session = session
        self.dynamodb_client = self.session.client("dynamodb")

    def get_item(self, table_name: str, key: dict, **kwargs) -> dict:
        return self.dynamodb_client.get_item(Key=key, TableName=table_name,
                                             **kwargs)["Item"]

    def update_item(self, table_name: str, key: dict, **kwargs):
        return self.dynamodb_client.update_item(TableName=table_name, Key=key,
                                                **kwargs)

    def scan_table(self, table_name, **kwargs):
        return self.dynamodb_client.scan(TableName=table_name, **kwargs)

    def close(self):
        self.dynamodb_client.close()


class DynamoDBPandas(DynamoDB):
    def __init__(self, session):
        super().__init__(session)

    def get_table(self, item_list: list, table_name: str) -> "dynamodb.Table":
        return wr.dynamodb.get_table(items=item_list, table_name=table_name,
                                     boto3_session=self.session)

    def put_items(self, item_list: list, table_name: str) -> None:
        wr.dynamodb.put_items(items=item_list, table_name=table_name,
                              boto3_session=self.session)

    def delete_items(self, item_list: list, table_name: str) -> None:
        wr.dynamodb.delete_items(items=item_list, table_name=table_name,
                                 boto3_session=self.session)
