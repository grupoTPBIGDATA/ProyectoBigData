from flask import Flask, request, Response
from flask_pymongo import PyMongo
from pycoingecko import CoinGeckoAPI
from bson import json_util
from datetime import date, datetime, timedelta, timezone
import tweepy
import json
from connectionChain import (cosumer_key,consumer_secret,access_token,access_token_secret)

from models import Precio

cg = CoinGeckoAPI()
app = Flask(__name__)
app.config['MONGO_URI'] = 'mongodb://localhost/proyectoBigData'
mongo = PyMongo(app)

# Cadenas de conexion 
auth = tweepy.OAuthHandler(cosumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

apiTwitter = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)

# Para ver si me trae lo de Elon Musk
data = apiTwitter.get_user("CryptoWhale")
#print (json.dumps(data._json,indent=2))

#Obtener los tweets
#for tweet in tweepy.Cursor(apiTwitter.user_timeline,screen_name = "elonmusk", tweet_mode = "extended").items(1):
#    print (json.dumps(tweet._json,indent=2))

@app.route('/tweets',methods=['GET'])
def getTweetsCripto():
    lista = []
    query = request.args['query']
    quantity = request.args['quantity']
    year = request.args['year']
    month = request.args['month']
    day = request.args['day']
    #for tweet in tweepy.Cursor(apiTwitter.search,q = query, tweet_mode = "extended").items(10):
        #json.dumps(tweet._json,indent=2)
        #print (tweet._json['created_at'] + ' ' + str(tweet._json['user']['screen_name']))
    for tweet in tweepy.Cursor(apiTwitter.search,q = query, until = date(int(year),int(month),int(day)).isoformat() ,tweet_mode = "extended").items(int(quantity)):
        if str(tweet._json['user']['screen_name']) == 'CryptoWhale': 
            tweets = {
                'created_at': tweet._json['created_at'],
                'user_name': str(tweet._json['user']['screen_name']),
                'profile_name': str(tweet._json['user']['name']),
                'profile_description':str(tweet._json['user']['description']) ,
                'full_text': tweet._json['full_text'],
                'hastag': tweet._json['entities']['hashtags'],
                'keyword' : query
                }
            lista.append(tweets)
            response = json_util.dumps(lista)
            mongo.db.tweetsCripto.insert(lista)
            return Response(response,mimetype='aplication/json')

@app.route('/tweetsTest/<userId>',methods=['GET'])
def test(userId):
    lista = []
    for tweet in apiTwitter.user_timeline(screen_name = userId, count = 200, include_rts = False, tweet_mode = "extended"):
        lista.append(json.dumps(tweet._json,indent=2))
    response = lista
    return Response(response,mimetype='aplication/json')

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

    if days_ago == 'max':
        date_since = days_ago
    else:
        date_since = datetime.today() - timedelta(days=float(days_ago))

    historial = cg.get_coin_market_chart_by_id(id=id_moneda, vs_currency='usd', days=days_ago, interval=time_interval)

    precios = historial['prices']

    # prices collection: price, datetime, coin
    lista_precios = [vars(Precio(id_moneda, x[1], datetime.fromtimestamp(x[0]/1000, timezone.utc))) for x in precios]

    mongo.db.priceHistory.insert(lista_precios)

    return {'message': 'history retrieved'}


if __name__ == "__main__":
    app.run(debug=True)
