from flask import Flask,request,Response
from flask_pymongo import PyMongo
from pycoingecko import CoinGeckoAPI
from bson import json_util

cg = CoinGeckoAPI()
app = Flask(__name__)
app.config['MONGO_URI']='mongodb://localhost/proyectoBigData'
mongo = PyMongo(app)

@app.route('/criptomonedas', methods=['POST'])
def create_cripto():
   #Recive data
   criptomonedas = cg.get_coins_list()
   mongo.db.Listacriptomonedas.insert(criptomonedas)       
   #print(request.json)
   return {'message':'received'}

@app.route('/criptomonedas',methods=['GET'])
def get_Criptomonedas():
   criptomonedas = cg.get_coins_list()
   print(len(criptomonedas))
   response = json_util.dumps(criptomonedas)
   #for i in range(len(criptomonedas)):
   #    print(criptomonedas[i])
   return Response(response, mimetype='aplication/json')

@app.route('/criptomonedas/<id>',methods=['GET'])
def get_Criptomoneda(id):
    print(id)
    return {'message':'received'}


if __name__ == "__main__":
    app.run(debug=True)