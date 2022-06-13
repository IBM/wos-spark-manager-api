from flask import Flask, request, abort, jsonify
import json
import base64
import os
import requests
import io
from subprocess import run
from delegation_token_generator import get_delegation_token

app = Flask(__name__)


@app.route('/delegation_token', methods=['GET'])
def delegation_token_generator():
    try:
        delegation_token = get_delegation_token()

        response = {
            "delegationToken": delegation_token
        }

        return jsonify(response)
    except Exception as ex:
        raise ex


@app.errorhandler(Exception)
def error_handler(e):
    return {"message": str(e)}, 500


if __name__ == '__main__':
    app.run(host='host1.company.fyre.ibm.com', port=9443, debug=True)
