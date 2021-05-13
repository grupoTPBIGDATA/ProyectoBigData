from flask import Flask, request, Response
from flask_pymongo import PyMongo
from pycoingecko import CoinGeckoAPI
from bson import json_util

cg = CoinGeckoAPI()
app = Flask(__name__)
app.config['MONGO_URI'] = 'mongodb://localhost/proyectoBigData'
mongo = PyMongo(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/criptomonedas', methods=['POST'])
def create_cripto():
    # Recive data
    criptomonedas = cg.get_coins_list()
    mongo.db.criptomonedas.insert(criptomonedas)
    # print(request.json)
    return {'message': 'received'}


@app.route('/criptomonedas', methods=['GET'])
def get_criptomonedas():
    # criptomonedas = cg.get_coins_list()
    # print(len(criptomonedas))
    # for i in range(len(criptomonedas)):
    #    print(criptomonedas[i])
    lista_criptomonedas = mongo.db.criptomonedas.find()
    response = json_util.dumps(lista_criptomonedas)
    return Response(response, mimetype='aplication/json')


@app.route('/criptomonedas/<id_moneda>', methods=['GET'])
def get_criptomoneda(id_moneda):
    print(id_moneda)
    return {'message': 'received'}


if __name__ == "__main__":
    app.run(debug=True)
