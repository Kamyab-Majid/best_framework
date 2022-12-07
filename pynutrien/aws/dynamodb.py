from __future__ import annotations

import typing

import boto3

from pynutrien.aws.boto import get_boto_session

if typing.TYPE_CHECKING:
    import awswrangler


class DynamoDB:
    """The DynamoDB wrapper."""

    def __init__(self, session: boto3.session.Session = None):
        """Initializing the DynamoDB given the boto3 session.

        Args:
            session (boto3.session.Session): The boto3 session to be used
        """
        self.session: boto3.session.Session = session or get_boto_session()
        self.dynamodb_client = self.session.client("dynamodb")

    def get_item(self, table_name: str, key: dict, **kwargs) -> dict:
        """Gets the item in a table given the key

        Args:
            table_name (str): The table name
            key (dict): The keys to search for

        Returns:
            dict: The result items.
        """
        return self.dynamodb_client.get_item(Key=key, TableName=table_name, **kwargs)["Item"]

    def update_item(self, table_name: str, key: dict, **kwargs) -> dict:
        """updats the item in the table

        Args:
            table_name (str): the table to do the update
            key (dict): the key to do the update

        Returns:
            dict: The response dictionay.
        """
        return self.dynamodb_client.update_item(TableName=table_name, Key=key, **kwargs)

    def scan_table(self, table_name: str, **kwargs) -> dict:
        """The Scan operation returns one or more items and item attributes by accessing every item in a table
        or a secondary index. To have DynamoDB return fewer items, you can provide a FilterExpression operation.

        Args:
            table_name (str): the name of the table

        Returns:
            _type_: _description_
        """
        return self.dynamodb_client.scan(TableName=table_name, **kwargs)

    def close(self):
        """closes the client."""
        self.dynamodb_client.close()


class DynamoDBPandas(DynamoDB):
    """The

    Args:
        DynamoDB (_type_): _description_
    """

    def __init__(self, session: boto3.session.Session):
        """initiating using the session

        Args:
            session (boto3.session.Session): boto3 session.
        """
        super().__init__(session)

    def get_table(self, table_name: str) -> "awswrangler.dynamodb.Table":
        """Gets a dynamo db table object, given the table name.

        Args:
            table_name (str): The name of the table

        Returns:
            dynamodb.Table: The dynamo db table object.
        """
        import awswrangler as wr

        return wr.dynamodb.get_table(table_name=table_name, boto3_session=self.session)

    def put_items(self, item_list: list, table_name: str) -> None:
        """puts the given items in the table.

        Args:
            item_list (list): The item list.
            table_name (str): The table name.
        """
        import awswrangler as wr

        wr.dynamodb.put_items(items=item_list, table_name=table_name, boto3_session=self.session)

    def delete_items(self, item_list: list, table_name: str) -> None:
        """Deleting items in the table

        Args:
            item_list (list): the item list to be deleted
            table_name (str): The table name to apply the delete action.
        """
        import awswrangler as wr

        wr.dynamodb.delete_items(items=item_list, table_name=table_name, boto3_session=self.session)
