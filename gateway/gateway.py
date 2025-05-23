# curl -X POST http://localhost:1337/auth/login -H "Content-Type: application/json" -d @auth.json
from flask import Flask, request, jsonify
from flask_jwt_extended import (
    JWTManager, create_access_token, jwt_required, get_jwt_identity
)
import os
import requests

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY', 'super-secret-key')
jwt = JWTManager(app)

HARDCODED_USER = {'username': 'user', 'password': 'user'}

@app.route('/auth/login', methods=['POST'])
def login():
    data = request.get_json(force=True)
    if data.get('username') != HARDCODED_USER['username'] or data.get('password') != HARDCODED_USER['password']:
        return jsonify({'msg': 'Неверные учетные данные'}), 401
    token = create_access_token(identity=data['username'])
    return jsonify(access_token=token), 200

def forward_request(lab_number):
    base_url = os.getenv(f'LAB{lab_number}_URL')
    resp = requests.post(
        f"{base_url}/api/lab{lab_number}/{ 'report' if lab_number == 1 else lab_number == 2 and 'audience_report' or 'group_report' }",
        json=request.get_json(force=True),
        headers={'Content-Type': 'application/json'}
    )
    return jsonify(resp.json()), resp.status_code

@app.route('/api/lab1/report', methods=['POST'])
@jwt_required()
def proxy_lab1():
    return forward_request(1)

@app.route('/api/lab2/audience_report', methods=['POST'])
@jwt_required()
def proxy_lab2():
    return forward_request(2)

@app.route('/api/lab3/group_report', methods=['POST'])
@jwt_required()
def proxy_lab3():
    return forward_request(3)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1337)
