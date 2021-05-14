from flask import Flask, request, Response
from flask_pymongo import PyMongo
from pycoingecko import CoinGeckoAPI
from bson import json_util
from datetime import datetime, timedelta, timezone

from models import Precio

cg = CoinGeckoAPI()
app = Flask(__name__)
app.config['MONGO_URI'] = 'mongodb://localhost/proyectoBigData'
mongo = PyMongo(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/criptomonedas', methods=['POST'])
def create_cripto():
    # Receive data
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


@app.route('/historial/<id_moneda>', methods=['POST'])
def create_history(id_moneda):
    days_ago = request.args['days']

    time_interval = request.args['interval']

    if days_ago == 'max':
        date_since = days_ago
    else:
        date_since = datetime.today() - timedelta(days=float(days_ago))

    historial = cg.get_coin_market_chart_by_id(id=id_moneda, vs_currency='usd', days=days_ago, interval=time_interval)

    precios = historial['prices']

    # prices collection: price, datetime, coin
    lista_precios = [Precio(id_moneda, x[1], datetime.fromtimestamp(x[0]/1000, timezone.utc)).__dict__ for x in precios]

    mongo.db.priceHistory.insert(lista_precios)

    return {'message': 'history retrieved'}


if __name__ == "__main__":
    app.run(debug=True)
