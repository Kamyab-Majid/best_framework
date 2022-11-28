from __future__ import annotations

import boto3
import awswrangler as wr


class DynamoDB:
    """The DynamoDB wrapper."""

    def __init__(self, session: boto3.session.Session):
        """Initializing the DynamoDB given the boto3 session.

        Args:
            session (boto3.session.Session): The boto3 session to be used
        """
        self.session: boto3.session.Session = session
        self.dynamodb_client = self.session.client("dynamodb")

    def get_item(self, table_name: str, key: dict, **kwargs) -> dict:
        """Gets the item in a table given the key

        Args:
            table_name (str): The table name
            key (dict): The keys to search for

        Returns:
            dict: The result items.
        """
        return self.dynamodb_client.get_item(Key=key, TableName=table_name, **kwargs)[
            "Item"
        ]

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
        """The Scan operation returns one or more items and item attributes by accessing every item in a table or a secondary index.
         To have DynamoDB return fewer items, you can provide a FilterExpression operation.

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

    def get_table(self, table_name: str) -> "dynamodb.Table":
        """Gets a dynamo db table object, given the table name.

        Args:
            table_name (str): The name of the table

        Returns:
            dynamodb.Table: The dynamo db table object.
        """
        return wr.dynamodb.get_table(table_name=table_name, boto3_session=self.session)

    def put_items(self, item_list: list, table_name: str) -> None:
        """puts the given items in the table.

        Args:
            item_list (list): The item list.
            table_name (str): The table name.
        """
        wr.dynamodb.put_items(
            items=item_list, table_name=table_name, boto3_session=self.session
        )

    def delete_items(self, item_list: list, table_name: str) -> None:
        """Deleting items in the table

        Args:
            item_list (list): the item list to be deleted
            table_name (str): The table name to apply the delete action.
        """
        wr.dynamodb.delete_items(
            items=item_list, table_name=table_name, boto3_session=self.session
        )


if __name__ == "__main__":
    session = boto3.session.Session()
    client = session.client("dynamodb")
    response = client.create_table(
        AttributeDefinitions=[
            {
                "AttributeName": "Artist",
                "AttributeType": "S",
            },
            {
                "AttributeName": "SongTitle",
                "AttributeType": "S",
            },
        ],
        KeySchema=[
            {
                "AttributeName": "Artist",
                "KeyType": "HASH",
            },
            {
                "AttributeName": "SongTitle",
                "KeyType": "RANGE",
            },
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        },
        TableName="Music",
    )
    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName="Music", WaiterConfig={"Delay": 1, "MaxAttempts": 100})
    response = client.batch_write_item(
        RequestItems={
            "Music": [
                {
                    "PutRequest": {
                        "Item": {
                            "AlbumTitle": {
                                "S": "Somewhat Famous",
                            },
                            "Artist": {
                                "S": "No One You Know",
                            },
                            "SongTitle": {
                                "S": "Call Me Today",
                            },
                        },
                    },
                },
                {
                    "PutRequest": {
                        "Item": {
                            "AlbumTitle": {
                                "S": "Songs About Life",
                            },
                            "Artist": {
                                "S": "Acme Band",
                            },
                            "SongTitle": {
                                "S": "Happy Day",
                            },
                        },
                    },
                },
                {
                    "PutRequest": {
                        "Item": {
                            "AlbumTitle": {
                                "S": "Blue Sky Blues",
                            },
                            "Artist": {
                                "S": "No One You Know",
                            },
                            "SongTitle": {
                                "S": "Scared of My Shadow",
                            },
                        },
                    },
                },
            ],
        },
    )
    response = client.put_item(
        Item={
            "AlbumTitle": {
                "S": "Somewhat Famous",
            },
            "Artist": {
                "S": "No One You Know",
            },
            "SongTitle": {
                "S": "Call Me Today",
            },
        },
        ReturnConsumedCapacity="TOTAL",
        TableName="Music",
    )
    my_dyno = DynamoDB(session)
    response_get = client.get_item(
        Key={
            "Artist": {
                "S": "Acme Band",
            },
            "SongTitle": {
                "S": "Happy Day",
            },
        },
        TableName="Music",
    )
    my_dyno_get = my_dyno.get_item(
        key={
            "Artist": {
                "S": "Acme Band",
            },
            "SongTitle": {
                "S": "Happy Day",
            },
        },
        table_name="Music",
    )
    assert my_dyno_get == response_get["Item"]
    update_response = my_dyno.update_item(
        ExpressionAttributeNames={
            "#AT": "AlbumTitle",
            "#Y": "Year",
        },
        ExpressionAttributeValues={
            ":t": {
                "S": "Louder Than Ever",
            },
            ":y": {
                "N": "2015",
            },
        },
        key={
            "Artist": {
                "S": "Acme Band",
            },
            "SongTitle": {
                "S": "Happy Day",
            },
        },
        ReturnValues="ALL_NEW",
        table_name="Music",
        UpdateExpression="SET #Y = :y, #AT = :t",
    )
    assert update_response["Attributes"] == {
        "AlbumTitle": {"S": "Louder Than Ever"},
        "Artist": {"S": "Acme Band"},
        "Year": {"N": "2015"},
        "SongTitle": {"S": "Happy Day"},
    }
    scan_response = my_dyno.scan_table(table_name="Music")
    response = client.scan(TableName="Music")
    assert response["Items"] == scan_response["Items"]
    my_dyno_pd = DynamoDBPandas(session)
    dynamo_table = my_dyno_pd.get_table(table_name="Music")
    assert dynamo_table.name == "Music"
    my_dyno_pd.put_items(
        table_name="Music",
        item_list=[
            {"Artist": "Nobody", "SongTitle": "Not Famous"},
            {"Artist": "Nicho", "SongTitle": "Bad song"},
        ],
    )
    assert dynamo_table.get_item(Key={"Artist": "Nobody", "SongTitle": "Not Famous"})[
        "Item"
    ] == {"Artist": "Nobody", "SongTitle": "Not Famous"}
    my_dyno_pd.delete_items(
        table_name="Music",
        item_list=[
            {"Artist": "Nobody", "SongTitle": "Not Famous"},
            {"Artist": "Nicho", "SongTitle": "Bad song"},
        ],
    )
    after_del_resp = dynamo_table.get_item(
        Key={"Artist": "Nobody", "SongTitle": "Not Famous"}
    )
    assert "Item" not in after_del_resp
    response = client.delete_table(TableName="Music")
