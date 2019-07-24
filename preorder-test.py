from sklearn.metrics import accuracy_score, log_loss
from sklearn.externals import joblib
import pandas as pd

filename = 'dt_model.sav'
loaded_model = joblib.load(filename)
X_test = pd.read_csv('x_test.csv')

del X_test['Unnamed: 0']
y_test = pd.read_csv('y_test.csv', header=None)
train_predictions = loaded_model.predict(X_test)

acc = accuracy_score(y_test[[1]], train_predictions)
print('knn accuracy is ' +str(acc))

print('some random probabilities')


X_test.set_index(X_test.iloc[:,0], inplace=True)
#X_test.head()
#loaded_model.predict_proba([X_test.at[19023,:]])
print(X_test)
print(X_test.iloc[10753])


filename = 'dt_model.sav'
loaded_model = joblib.load(filename)

train_predictions = loaded_model.predict(X_test)



acc = accuracy_score(y_test[[1]], train_predictions)
#print('decision trees accuracy is ' +str(acc))
