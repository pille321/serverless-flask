import os

import boto3
from flask import Flask, jsonify, request, json

BOOKS_TABLE = os.environ['BOOKS_TABLE']
client = boto3.client('dynamodb')
resource = boto3.resource('dynamodb')

BOOKS_SNS_TOPIC = os.environ['BOOKS_SNS_TOPIC']
snsclient = boto3.client('sns')

app = Flask(__name__)
app.config["DEBUG"] = True


# A route to return all of the available entries in our catalog.
#

def publish_to_topic(message):
    snsclient.publish(
        TargetArn=BOOKS_SNS_TOPIC,
        Message=json.dumps(message)
    )


@app.route("/")
@app.route("/api/v1/books")
def get_books():
    table = resource.Table(BOOKS_TABLE)
    resp = table.scan()
    return jsonify(resp.get('Items'))


@app.route("/api/v1/book/<string:book_id>")
def get_book(book_id):
    resp = client.get_item(
        TableName=BOOKS_TABLE,
        Key={
            'bookId': {'S': book_id}
        }
    )
    item = resp.get('Item')
    if not item:
        return jsonify({'error': 'Book does not exist'}), 404

    return jsonify({
        'bookId': item.get('bookId').get('S'),
        'title': item.get('title').get('S')
    })


@app.route("/api/v1/book", methods=["POST"])
def creat_book():
    book_id = request.json.get('bookId')
    title = request.json.get('title')
    if not book_id or not title:
        return jsonify({'error': 'Please provide bookId and title'}), 400

    resp = client.put_item(
        TableName=BOOKS_TABLE,
        Item={
            'bookId': {'S': book_id},
            'title': {'S': title}
        }
    )

    book_json = {
        'bookId': book_id,
        'title': title
    }

    publish_to_topic(book_json)
    return jsonify(book_json)
