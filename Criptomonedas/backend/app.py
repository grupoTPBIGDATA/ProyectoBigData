from datetime import date, datetime, timezone

import tweepy
import twint
from bson import json_util
from flask import Flask, request, Response
from flask_pymongo import PyMongo, ObjectId
from pycoingecko import CoinGeckoAPI
from pytwitterscraper import TwitterScraper
import pymongo
import csv
import pandas as pd
import time

from connectionChain import (cosumer_key, consumer_secret, access_token, access_token_secret)
from models import Precio, Tweet,Variation

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

tw = TwitterScraper()


# profile = tw.get_profile(name="elonmusk")
# print(profile.__dict__)
# search = tw.searchkeywords("dogecoin")
# print(search.__dict__)
# user = search.__dict__
# print(user['users'])
# for u in user['users']:
#    print(u['name'])
# for s in search.__dict__:
#    print(str(s['name']))


# data = tw.get_profile(names=["Dogecoin"])
# for data_mem in data :
#    print(data_mem.id)

# tweets = tw.get_tweets(2235729541, count=9000)
# print(tweets.contents)


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


@app.route('/tweets/profiles', methods=['GET'])
def get_tweets_profiles():
    keyword = request.args['keyword']
    search = tw.searchkeywords(keyword)
    user = vars(search)
    lista = []
    for u in user['users']:
        print(u['screen_name'])
        data = tw.get_profile(names=[u['screen_name']])
        for data_mem in data:
            print(data_mem.id)
            tweets = tw.get_tweets(int(data_mem.id), count=9000)
            tweet = {
                'user': u['screen_name'],
                'tweet': tweets.contents
            }
            lista.append(tweet)
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


@app.route('/search', methods=['POST'])
def search_keyword():
    keywords = request.json["keywords"]
    lista_tweets = []
    config = twint.Config()
    config.Retweets = False
    config.Min_likes = 5000
    config.Hide_output = True
    config.Store_object = True
    #config.Format = "ID {id} | Name {name}"

    for word in keywords:
        config.Search = word
        twint.run.Search(config)
        tweets = twint.output.tweets_list

        tweets_clean = [vars(Tweet(x.id, x.tweet, x.hashtags, x.cashtags,
                                   datetime.strptime(x.datestamp + x.timestamp + x.timezone, '%Y-%m-%d%H:%M:%S%z'),
                                   x.username, x.name, x.link, word)) for x in tweets]

        lista_tweets.extend(tweets_clean)

    mongo.db.tweetsCripto.insert(lista_tweets)

    return Response(f'{len(lista_tweets)} tweets insertados en db.tweetsCripto', mimetype='aplication/json')


@app.route('/searchTweetCripto', methods=['GET'])
def searchTweetCripto():
    keywords1 = [
        "bitcoin",
        "BTC"
    ]
    keywords2 = [
        "ethereum",
        "ETH"
    ]
    keywords3 = [
        "dogecoin",
        "DOGE"
    ]
    start = time.process_time()

    pandas_results = []
    
    cryptocurrency_variation = []
    
    cryptocurrencies_above = 0

    custom_date_parser = lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f%z")

    tweet_cryptocurrency_variation = []

    reader = pd.read_csv('tweets_result.csv',encoding="utf8",parse_dates=['fecha'], date_parser=custom_date_parser)
    
    for index, row in reader.iterrows():
        pandas_results.append(row)
        #print(row.fecha.day)

    end = time.process_time()
    print(f"Tiempo pandas {end - start}")

    start = time.process_time()

    results = []

    with open('tweets_result.csv', encoding="utf8") as File:
        reader = csv.DictReader(File)
        for row in reader:
            row.update({'fecha': datetime.strptime(row['fecha'], '%Y-%m-%dT%H:%M:%S.%f%z')})
            results.append(row)

        end = time.process_time()
        print(f"Tiempo csv {end - start}")

    for x in mongo.db.volumeHistory.find():
        if(cryptocurrencies_above != 0):
            variation = ((x['volume'] - cryptocurrencies_above)/cryptocurrencies_above)*100
            if(variation >= 5): 
                #x_variation = [vars(Variation(y['id_criptomoneda'],y['volume'],datetime.strptime(y['fecha'], '%Y-%m-%dT%H:%M:%S.%f%z'),'Mayor al 5%',variation)) for y in x]
                x_variation = {
                    'id_criptomoneda':x['id_criptomoneda'],
                    'volume': x['volume'],
                    'fecha': datetime.strftime(x['fecha'], '%Y-%m-%dT%H:%M:%S.%f%z'),
                    'type_variation': 'mayor al 5 %',
                    'type_code': 1,
                    'percentage_variation': variation
                }
                cryptocurrency_variation.append(x_variation)
            
            if(-5 < variation < 5 ):
                x_variation = {
                    'id_criptomoneda':x['id_criptomoneda'],
                    'volume': x['volume'],
                    'fecha': datetime.strftime(x['fecha'], '%Y-%m-%dT%H:%M:%S.%f%z'),
                    'type_variation': 'entre -5% y 5%',
                    'type_code': 0,
                    'percentage_variation': variation
                }
                cryptocurrency_variation.append(x_variation)

            if(variation <=  -5):
                #x_variation = [vars(Variation(y['id_criptomoneda'],y['volume'],datetime.strptime(y['fecha'], '%Y-%m-%dT%H:%M:%S.%f%z'),'Menor al 5%',variation)) for y in x]
                x_variation = {
                    'id_criptomoneda':x['id_criptomoneda'],
                    'volume': x['volume'],
                    'fecha': datetime.strftime(x['fecha'], '%Y-%m-%dT%H:%M:%S.%f%z'),
                    'type_variation': 'menor al -5 %',
                    'type_code': -1,
                    'percentage_variation': variation
                }
                cryptocurrency_variation.append(x_variation)
            for row in pandas_results:
                f1 = date(x['fecha'].year,x['fecha'].month,x['fecha'].day)
                f2 = date(row.fecha.year,row.fecha.month,row.fecha.day)
                diferenciaFecha = f1 - f2
                if( 0 < diferenciaFecha.days < 2):
                    if(x['id_criptomoneda'] == 'bitcoin'):
                        if any(keyword.upper() in row.tweet.upper() for keyword in keywords1):
                            tweet_variaton = {
                                'id_criptomoneda':x['id_criptomoneda'],
                                'percentage_variation': variation,
                                'type_code_variaton':x_variation['type_code'],
                                'id_twwet': row._id,
                                'fecha_tweet': row.fecha,
                                'link_tweet': row.link,
                                'userName_tweet': row.name,
                                'user_tweet': row.user,
                                'content_tweet': row.tweet,
                                'hashtag_tweet': row.hashtag,
                                'cashtag_tweet': row.cashtag
                                }
                    if(x['id_criptomoneda'] == 'dogecoin'):
                        if any(keyword.upper() in row.tweet.upper() for keyword in keywords3):
                            tweet_variaton = {
                                'id_criptomoneda':x['id_criptomoneda'],
                                'percentage_variation': variation,
                                'type_code_variaton':x_variation['type_code'],
                                'id_twwet': row._id,
                                'fecha_tweet': row.fecha,
                                'link_tweet': row.link,
                                'userName_tweet': row.name,
                                'user_tweet': row.user,
                                'content_tweet': row.tweet,
                                'hashtag_tweet': row.hashtag,
                                'cashtag_tweet': row.cashtag
                                }
                    if(x['id_criptomoneda'] == 'ethereum'):
                        if any(keyword.upper() in row.tweet.upper() for keyword in keywords2):
                            tweet_variaton = {
                                'id_criptomoneda':x['id_criptomoneda'],
                                'percentage_variation': variation,
                                'type_code_variaton':x_variation['type_code'],
                                'id_twwet': row._id,
                                'fecha_tweet': row.fecha,
                                'link_tweet': row.link,
                                'userName_tweet': row.name,
                                'user_tweet': row.user,
                                'content_tweet': row.tweet,
                                'hashtag_tweet': row.hashtag,
                                'cashtag_tweet': row.cashtag
                                }
                    tweet_cryptocurrency_variation.append(tweet_variaton)
                #if(x['fecha'].month == row.fecha.month and x['fecha'].year == row.fecha.year):
                    if((row.fecha.day <= x['fecha'].day - 2) and ((x['id_criptomoneda'] in row.hashtag) or (x['id_criptomoneda'] in row.cashtag) or (x['id_criptomoneda'] in row.tweet))):
                        #and x['id_criptomoneda'] in row.hashtag or x['id_criptomoneda'] in row.cashtag
                        tweet_variaton = {
                            'id_criptomoneda':x['id_criptomoneda'],
                            'percentage_variation': variation,
                            'type_code_variaton':x_variation['type_code'],
                            'id_twwet': row._id,
                            'fecha_tweet': row.fecha,
                            'link_tweet': row.link,
                            'userName_tweet': row.name,
                            'user_tweet': row.user,
                            'content_tweet': row.tweet,
                            'hashtag_tweet': row.hashtag,
                            'cashtag_tweet': row.cashtag
                        }
                        tweet_cryptocurrency_variation.append(tweet_variaton)
                #print(row)
                #print(x['fecha'].day)
        cryptocurrencies_above = x['volume']
    #print(cryptocurrency_variation)
    #mongo.db.criptocurrencyVariation.insert(cryptocurrency_variation)
    response = json_util.dumps(tweet_cryptocurrency_variation)
    print(tweet_cryptocurrency_variation)
    print(len(tweet_cryptocurrency_variation))
    return Response(response, mimetype='aplication/json')

if __name__ == "__main__":
    app.run(debug=True)
