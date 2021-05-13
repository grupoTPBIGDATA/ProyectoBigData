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
   print(request.json)
   return {'message':'received'}

@app.route('/criptomonedas',methods=['GET'])
def get_Criptomonedas():
   criptomonedas = cg.get_coins_list()
   print(criptomonedas)
   response = json_util.dumps(criptomonedas)
   return Response(response, mimetype='aplication/json')
   
if __name__ == "__main__":
    app.run(debug=True)