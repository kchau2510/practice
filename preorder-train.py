from datetime import datetime as dt
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, log_loss
import pandas as pd
from sklearn.externals import joblib

df_us1 = pd.read_excel(open('data/Feb2019.xls', 'rb'),sheet_name='Export Worksheet')

def preprocess(df_us1):
    pd.options.mode.chained_assignment  = None
    df_us1 = df_us1.dropna()
    df_us1 = df_us1[df_us1['ISVDC'] == 'nonvdc']
    df_us1['SUBMITTED_DATE'] = pd.to_datetime(df_us1['SUBMITTED_DATE'].str[:10])
    
    df_us1['day'] = df_us1['SUBMITTED_DATE'].dt.day
    df_us1['month'] = df_us1['SUBMITTED_DATE'].dt.month
    df_us1['year'] = df_us1['SUBMITTED_DATE'].dt.year
    
    df_us1['zip_group'] = df_us1['DELIVERY_ZIP_CODE'].str[:1]
    df_us1['zip_region'] = df_us1['DELIVERY_ZIP_CODE'].str[1:3]
    df_us1['zip_town'] = df_us1['DELIVERY_ZIP_CODE'].str[3:5]
    df_us1['zip_section'] = df_us1['DELIVERY_ZIP_CODE'].str[:3]
    df_us1['zip_narrow'] = df_us1['DELIVERY_ZIP_CODE'].str[5:]
    
    df_us2 = df_us1[['DELIVERY_CITY', 'STATE', 'COUNTRY', 'QUANTITY',
                 'CASE_WEIGHT',  'ISLTL', 'ISASSEMBLY', 'ISGIFTCARD', 
                 'JDA_DEPT', 'zip_group', 'zip_region', 'zip_town',
                 'zip_section', 'day', 'month', 'year', 'Source_ZIP']]
    
    df_us2['DELIVERY_CITY'] = pd.Categorical(df_us2['DELIVERY_CITY'])
    df_us2['STATE'] = pd.Categorical(df_us2['STATE'])
    df_us2['JDA_DEPT'] = pd.Categorical(df_us2['JDA_DEPT'])
    df_us2['day'] = pd.Categorical(df_us2['day'])
    df_us2['zip_region'] = pd.Categorical(df_us2['zip_region'])
    df_us2['zip_section'] = pd.Categorical(df_us2['zip_section'])
    df_us2['zip_town'] = pd.Categorical(df_us2['zip_town'])
    df_us2['zip_group'] = pd.Categorical(df_us2['zip_group'])
    df_us2['month'] = pd.Categorical(df_us2['month'])
    df_us2['year'] = pd.Categorical(df_us2['year'])

    df_us2.to_csv('df_us2_demo.csv')
    
    df_us2['DELIVERY_CITY'] = LabelEncoder().fit_transform(df_us2['DELIVERY_CITY'])
    df_us2['STATE'] = LabelEncoder().fit_transform(df_us2['STATE'])
    df_us2['JDA_DEPT'] = LabelEncoder().fit_transform(df_us2['JDA_DEPT'])
    df_us2['zip_group'] = LabelEncoder().fit_transform(df_us2['zip_group'].astype(str))
    df_us2['zip_region'] = LabelEncoder().fit_transform(df_us2['zip_region'].astype(str))
    df_us2['zip_town'] = LabelEncoder().fit_transform(df_us2['zip_town'].astype(str))
    df_us2['zip_section'] = LabelEncoder().fit_transform(df_us2['zip_section'].astype(str))
    df_us2['day'] = LabelEncoder().fit_transform(df_us2['day'].astype(str))
    df_us2['month'] = LabelEncoder().fit_transform(df_us2['month'].astype(str))
    df_us2['year'] = LabelEncoder().fit_transform(df_us2['year'].astype(str))
    le = LabelEncoder()
    df_us2['Source_ZIP'] = le.fit_transform(df_us2['Source_ZIP'])
    df_us2 = df_us2.drop(columns=['COUNTRY'])
    
    return(df_us2,le)


df_us2,le = preprocess(df_us1)

y = df_us2['Source_ZIP']
X = df_us2.drop('Source_ZIP', axis=1)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.4, random_state=42)

kclf = KNeighborsClassifier()
kclf.fit(X_train, y_train)

train_predictions = kclf.predict(X_test)
print('finished training done for knn model')
acc = accuracy_score(y_test, train_predictions)
print(acc)

filename = 'knn_model.sav'
joblib.dump(kclf, filename)

X_test.to_csv('x_test.csv')
y_test.to_csv('y_test.csv', header=False)

print('knn model saved as ' + 'filename')

dclf = DecisionTreeClassifier()
dclf.fit(X_train, y_train)
train_predictions = dclf.predict(X_test)
acc = accuracy_score(y_test, train_predictions)
print(acc)

filename = 'dt_model.sav'
joblib.dump(kclf, filename)

print('training done for decision trees model')
print('decision trees model saved as ' + filename)

