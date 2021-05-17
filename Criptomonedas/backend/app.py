from datetime import date, datetime, timezone

import tweepy
from bson import json_util
from flask import Flask, request, Response
from flask_pymongo import PyMongo
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


# print (json.dumps(data._json,indent=2))

# Obtener los tweets
# for tweet in tweepy.Cursor(apiTwitter.user_timeline,screen_name = "elonmusk", tweet_mode = "extended").items(1):
#    print (json.dumps(tweet._json,indent=2))

@app.route('/tweets', methods=['GET'])
def get_tweets_crypto():
    lista = []
    query = request.args['query']
    quantity = request.args['quantity']
    year = request.args['year']
    month = request.args['month']
    day = request.args['day']
    # for tweet in tweepy.Cursor(apiTwitter.search,q = query, tweet_mode = "extended").items(10):
    # json.dumps(tweet._json,indent=2)
    # print (tweet._json['created_at'] + ' ' + str(tweet._json['user']['screen_name']))
    for tweet in tweepy.Cursor(apiTwitter.search, q=query, until=date(int(year), int(month), int(day)).isoformat(),
                               tweet_mode="extended").items(int(quantity)):
        if tweet.user.screen_name == 'CryptoWhale':
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

    for i in range(int(ciclos)):

        if i == 0:
            tweets = apiTwitter.user_timeline(screen_name=user_id, count=200, include_rts=False, tweet_mode="extended")
        else:
            tweets = apiTwitter.user_timeline(screen_name=user_id, count=200, include_rts=False, tweet_mode="extended",
                                              max_id=min_id_last_fetch)

        for tweet in tweets:
            tweet_formatted = {
                'created_at': tweet.created_at,
                'user_name': tweet.user.screen_name,
                'profile_name': tweet.user.name,
                'full_text': tweet.full_text,
                'hashtag': tweet.entities['hashtags'],
                'id': tweet.id
            }
            lista.append(tweet_formatted)

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

    historial = cg.get_coin_market_chart_by_id(id=id_moneda, vs_currency='usd', days=days_ago, interval=time_interval)

    precios = historial['prices']

    # prices collection: price, datetime, coin
    lista_precios = [vars(Precio(id_moneda, x[1], datetime.fromtimestamp(x[0] / 1000, timezone.utc))) for x in precios]

    mongo.db.priceHistory.insert(lista_precios)

    return {'message': 'history retrieved'}


if __name__ == "__main__":
    app.run(debug=True)
