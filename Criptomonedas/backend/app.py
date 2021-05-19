from datetime import date, datetime, timezone

import tweepy
from bson import json_util
from flask import Flask, request, Response
from flask_pymongo import PyMongo, ObjectId
from pycoingecko import CoinGeckoAPI

from connectionChain import (cosumer_key, consumer_secret, access_token, access_token_secret)
from models import Precio

cg = CoinGeckoAPI()
app = Flask(__name__)
app.config['MONGO_URI'] = 'mongodb://localhost/proyectoBigData'
mongo = PyMongo(app)

# Cadenas de conexion 
auth = tweepy.OAuthHandler(cosumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

apiTwitter = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

# Para ver si me trae lo de Elon Musk
data = apiTwitter.get_user("CryptoWhale")


@app.route('/tweets', methods=['GET'])
def get_tweets_crypto():
    lista = []
    query = request.args['query']
    quantity = request.args['quantity']
    year = request.args['year']
    month = request.args['month']
    day = request.args['day']
    for tweet in tweepy.Cursor(apiTwitter.search, q=query, until=date(int(year), int(month), int(day)).isoformat(),
                               tweet_mode="extended").items(int(quantity)):
        tweets = {
            'created_at': tweet.created_at,
            'user_name': tweet.user.screen_name,
            'profile_name': tweet.user.name,
            'profile_description': tweet.user.description,
            'full_text': tweet.full_text,
            'hashtag': tweet.entities['hashtags'],
            'keyword': query
        }
        lista.append(tweets)
        mongo.db.tweetsCripto.insert(lista)

    response = json_util.dumps(lista)
    return Response(response, mimetype='aplication/json')


@app.route('/tweets/<user_id>', methods=['GET'])
def get_user_tweets(user_id):
    ciclos = request.args['ciclos']

    lista = []

    min_id_last_fetch = 0

    for i in range(int(ciclos)):

        if i == 0:
            tweets = apiTwitter.user_timeline(screen_name=user_id, count=200, include_rts=False,
                                              tweet_mode="extended")
        else:
            tweets = apiTwitter.user_timeline(screen_name=user_id, count=200, include_rts=False,
                                              tweet_mode="extended", max_id=min_id_last_fetch)

        for tweet in tweets:
            tweet_formatted = {
                'created_at': tweet.created_at,
                'user_name': tweet.user.screen_name,
                'profile_name': tweet.user.name,
                'full_text': tweet.full_text,
                'hashtag': tweet.entities['hashtags'],
                'id': tweet.id
            }

            print(tweet.created_at)

            lista.append(tweet_formatted)

            if min_id_last_fetch == 0 or tweet.id < min_id_last_fetch:
                min_id_last_fetch = tweet.id

    mongo.db.tweetsCripto.insert(lista)

    return Response(json_util.dumps(lista), mimetype='aplication/json')


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/criptomonedas', methods=['POST'])
def create_cripto():
    # Receive data
    criptomonedas = cg.get_coins_list()
    mongo.db.criptomonedas.insert(criptomonedas)
    return {'message': 'received'}


@app.route('/criptomonedas', methods=['GET'])
def get_criptomonedas():
    lista_criptomonedas = mongo.db.criptomonedas.find()
    response = json_util.dumps(lista_criptomonedas)
    return Response(response, mimetype='aplication/json')


@app.route('/criptomonedas/<id_moneda>', methods=['GET'])
def get_criptomonedaPrice(id_moneda):
    criptomoneda = cg.get_price(ids=id_moneda, vs_currencies='usd', include_market_cap=True, include_24hr_vol=True,
                                include_24hr_change=True, include_last_updated_at=True)
    return criptomoneda


@app.route('/historial/<id_moneda>', methods=['POST'])
def create_history(id_moneda):
    days_ago = request.args['days']

    time_interval = request.args['interval']

    historial = cg.get_coin_market_chart_by_id(id=id_moneda, vs_currency='usd', days=days_ago, interval=time_interval)

    precios = historial['prices']

    # prices collection: price, datetime, coin
    lista_precios = [vars(Precio(id_moneda, x[1], datetime.fromtimestamp(x[0] / 1000, timezone.utc))) for x in precios]

    mongo.db.priceHistory.insert(lista_precios)

    return {'message': 'history retrieved'}


@app.route('/historial/<id_moneda>', methods=['GET'])
def get_history(id_moneda):
    # prices collection: price, datetime, coin
    lista_precio_criptomonedas = []
    for precio_criptomonedas in mongo.db.priceHistory.find({'id_criptomoneda': id_moneda}):
        lista_precio_criptomonedas.append({
            '_id': str(ObjectId(precio_criptomonedas['_id'])),
            'id_criptomoneda': precio_criptomonedas['id_criptomoneda'],
            'precio': precio_criptomonedas['precio'],
            'fecha': datetime.strftime(precio_criptomonedas['fecha'], '%Y-%m-%dT%H:%M:%S.%f%z')
        })
    response = json_util.dumps(lista_precio_criptomonedas)
    return Response(response, mimetype='aplication/json')


@app.route('/historial/<id_moneda>', methods=['PUT'])
def update_history(id_moneda):
    days_ago = request.args['days']
    time_interval = request.args['interval']
    mongo.db.priceHistory.delete_many({'id_criptomoneda': id_moneda})
    historial = cg.get_coin_market_chart_by_id(id=id_moneda, vs_currency='usd', days=days_ago, interval=time_interval)
    precios = historial['prices']

    # prices collection: price, datetime, coin
    lista_precios = [vars(Precio(id_moneda, x[1], datetime.fromtimestamp(x[0] / 1000, timezone.utc))) for x in precios]
    mongo.db.priceHistory.insert(lista_precios)
    return {'message': 'history updated'}


if __name__ == "__main__":
    app.run(debug=True)
