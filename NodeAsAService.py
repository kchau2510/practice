from sklearn.metrics import accuracy_score, log_loss
from sklearn.externals import joblib
import pandas as pd
from flask import Flask, render_template
from flask import jsonify
import json
from flask import request

app = Flask(__name__)


@app.route('/node', methods=['GET'])
def get_source_node():
    X_test = pd.read_csv('x_test.csv')
    return X_test.loc[10753]

@app.route('/')
def hello_world():
   return "Hello World"

if __name__ == '__main__':
   app.run()
