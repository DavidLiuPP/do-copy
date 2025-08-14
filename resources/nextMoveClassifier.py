import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import classification_report, confusion_matrix
from sqlalchemy import create_engine
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import ConfusionMatrixDisplay
import xgboost as xgb
from sklearn.metrics import classification_report, accuracy_score
from datetime import datetime, timedelta



engine = create_engine('postgresql://drayos:AxZcKG5D63iQzpq@embedded-email-cluster-dev.cluster-c3k5hvhtcivp.us-east-1.rds.amazonaws.com/drayos')



available_loads_source_sql = '''
WITH date_ref AS (
    SELECT DATE %(reference_date)s AS ref_date
),available_loads AS(
select ld.reference_number, 
case when 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'PULLCONTAINER' THEN 1 ELSE 0 END)>0) and 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'DELIVERLOAD' THEN 1 ELSE 0 END)=0) and 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'RETURNCONTAINER' THEN 1 ELSE 0 END)=0) then 'Pre-Pull'
when 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'DELIVERLOAD' THEN 1 ELSE 0 END)>0) and 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'RETURNCONTAINER' THEN 1 ELSE 0 END)=0) then 'Deliver'
when 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'DELIVERLOAD' THEN 1 ELSE 0 END)>0) and 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'RETURNCONTAINER' THEN 1 ELSE 0 END)>0) then 'Deliver and Return'
else 'Do Nothing'
end as Next_Move 
from date_ref, "driverOrder" dor left join "loadData" ld 
on dor."loadId" = ld."id" 
where ld.carrier =  %(carrier)s
AND ld.type_of_load in ('IMPORT', 'EXPORT')
and ld.revenue !=0
and date(ld."availableDate") <= ref_date 
and ld."availableDate" is not null 
and coalesce(dor.enroute, ref_date) >=ref_date
group by ld.reference_number
having min(coalesce(dor.indx, 0))=0)
select  
al.reference_number,
al.Next_Move ,
ld.distance,
ld."callerName" , 
ld."shipperName" , 
ld."consigneeName" ,
ld.hazmat, 
ld.hot, 
ld.liquor , 
ld.revenue ,
EXTRACT(day FROM (ld."lastFreeDay" - ref_date)) AS "lastFreeDay", 
EXTRACT(day FROM (ld."emptyDay" - ref_date)) AS "emptyDay", 
EXTRACT(day FROM (ld."pickupFromTime" - ref_date)) AS "pickupFromTime", 
EXTRACT(day FROM (ld."pickupToTime" - ref_date)) AS "pickupToTime", 
EXTRACT(day FROM (ld."deliveryFromTime" - ref_date)) AS "deliveryFromTime", 
EXTRACT(day FROM (ld."deliveryToTime" - ref_date)) AS "deliveryToTime", 
EXTRACT(day FROM (ld."dischargedDate" - ref_date)) AS "dischargedDate",
EXTRACT(day FROM (ld."availableDate" - ref_date)) AS "availableDate",
EXTRACT(dow FROM ref_date) AS "DOW"
from date_ref, "loadData" ld right join available_loads al 
on ld.reference_number = al.reference_number
where ld.carrier =  %(carrier)s
AND ld."callerInfo" is not null
AND ld."shipperInfo" is not null
AND ld."consigneeInfo" is not null
and al.reference_number in (select reference_number from available_loads)
;'''




prepulled_loads_source_sql = '''
WITH date_ref AS (
    SELECT DATE %(reference_date)s AS ref_date
),prepulled_loads AS(
select ld.reference_number,
case when 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'DELIVERLOAD' THEN 1 ELSE 0 END)>0) and 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'RETURNCONTAINER' THEN 1 ELSE 0 END)=0) then 'Deliver'
when 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'DELIVERLOAD' THEN 1 ELSE 0 END)>0) and 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'RETURNCONTAINER' THEN 1 ELSE 0 END)>0) then 'Deliver and Return'
else 'Do Nothing' end as Next_Move 
from date_ref, "driverOrder" dor left join "loadData" ld 
on dor."loadId" = ld."id" 
where ld.carrier = %(carrier)s
AND ld.type_of_load in ('IMPORT', 'EXPORT')
and ld.revenue !=0
group by ld.reference_number 
HAVING SUM(CASE WHEN dor.type = 'PULLCONTAINER' and dor.enroute < ref_date THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN dor.type = 'DELIVERLOAD' and dor.enroute < ref_date THEN 1 ELSE 0 END) = 0
   AND SUM(CASE WHEN dor.type = 'RETURNCONTAINER' and dor.enroute < ref_date THEN 1 ELSE 0 END) = 0)
select  
pl.reference_number,
pl.Next_Move ,
ld.distance,
ld."callerName" , 
ld."shipperName" , 
ld."consigneeName" ,
ld.hazmat, 
ld.hot, 
ld.liquor , 
ld.revenue ,
EXTRACT(day FROM (ld."lastFreeDay" - ref_date)) AS "lastFreeDay", 
EXTRACT(day FROM (ld."emptyDay" - ref_date)) AS "emptyDay", 
EXTRACT(day FROM (ld."pickupFromTime" - ref_date)) AS "pickupFromTime", 
EXTRACT(day FROM (ld."pickupToTime" - ref_date)) AS "pickupToTime", 
EXTRACT(day FROM (ld."deliveryFromTime" - ref_date)) AS "deliveryFromTime", 
EXTRACT(day FROM (ld."deliveryToTime" - ref_date)) AS "deliveryToTime", 
EXTRACT(day FROM (ld."dischargedDate" - ref_date)) AS "dischargedDate",
EXTRACT(day FROM (ld."availableDate" - ref_date)) AS "availableDate",
EXTRACT(dow FROM ref_date) AS "DOW"
from date_ref, "loadData" ld right join prepulled_loads pl 
on ld.reference_number = pl.reference_number
where ld.carrier = %(carrier)s
AND ld."callerInfo" is not null
AND ld."shipperInfo" is not null
AND ld."consigneeInfo" is not null
and pl.reference_number in (select reference_number from prepulled_loads)
;'''



delivered_loads_source_sql = '''
WITH date_ref AS (
    SELECT DATE %(reference_date)s AS ref_date
),prepulled_loads AS(
select ld.reference_number,
case when 
(SUM(CASE WHEN date(dor.enroute) = ref_date and dor.type = 'RETURNCONTAINER' THEN 1 ELSE 0 END)>0) then 'Return'
else 'Do Nothing' end as Next_Move 
from date_ref, "driverOrder" dor left join "loadData" ld 
on dor."loadId" = ld."id" 
where ld.carrier = %(carrier)s
AND ld.type_of_load in ('IMPORT', 'EXPORT')
and ld.revenue !=0
group by ld.reference_number 
HAVING SUM(CASE WHEN dor.type = 'DELIVERLOAD' and dor.enroute < ref_date THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN dor.type = 'RETURNCONTAINER' and dor.enroute < ref_date THEN 1 ELSE 0 END) = 0)
select  
pl.reference_number,
pl.Next_Move ,
ld.distance,
ld."callerName" , 
ld."shipperName" , 
ld."consigneeName" ,
ld.hazmat, 
ld.hot, 
ld.liquor , 
ld.revenue ,
EXTRACT(day FROM (ld."lastFreeDay" - ref_date)) AS "lastFreeDay", 
EXTRACT(day FROM (ld."emptyDay" - ref_date)) AS "emptyDay", 
EXTRACT(day FROM (ld."pickupFromTime" - ref_date)) AS "pickupFromTime", 
EXTRACT(day FROM (ld."pickupToTime" - ref_date)) AS "pickupToTime", 
EXTRACT(day FROM (ld."deliveryFromTime" - ref_date)) AS "deliveryFromTime", 
EXTRACT(day FROM (ld."deliveryToTime" - ref_date)) AS "deliveryToTime", 
EXTRACT(day FROM (ld."returnFromTime" - ref_date)) AS "returnFromTime", 
EXTRACT(day FROM (ld."returnToTime" - ref_date)) AS "returnToTime", 
EXTRACT(day FROM (ld."dischargedDate" - ref_date)) AS "dischargedDate",
EXTRACT(day FROM (ld."availableDate" - ref_date)) AS "availableDate",
EXTRACT(dow FROM ref_date) AS "DOW"
from date_ref, "loadData" ld right join prepulled_loads pl 
on ld.reference_number = pl.reference_number
where ld.carrier = %(carrier)s
AND ld."callerInfo" is not null
AND ld."shipperInfo" is not null
AND ld."consigneeInfo" is not null
and pl.reference_number in (select reference_number from prepulled_loads)
;'''


def next_move_classifier(loads_type, carrier_name, target_date):
    if carrier_name =='RoadEx':
        carrier_id = '641a10875b159a160742327e'
    elif carrier_name =='Best Drayage':
        carrier_id = '6509badea6dea315ddddd273'

    sql_params_train_list = [{
    'carrier': carrier_id,
    'reference_date': (datetime.strptime(target_date, '%Y-%m-%d')- timedelta(days=7)).strftime('%Y-%m-%d')
    },
    {
    'carrier': carrier_id,
    'reference_date': (datetime.strptime(target_date, '%Y-%m-%d')- timedelta(days=14)).strftime('%Y-%m-%d')
    },
    {
    'carrier': carrier_id,
    'reference_date': (datetime.strptime(target_date, '%Y-%m-%d')- timedelta(days=21)).strftime('%Y-%m-%d')
    },
    {
    'carrier': carrier_id,
    'reference_date': (datetime.strptime(target_date, '%Y-%m-%d')- timedelta(days=28)).strftime('%Y-%m-%d')
    }
    ]


    sql_params_test = {
    'carrier': carrier_id,
    'reference_date': target_date
    }


    if loads_type == 'Available Loads':
        query = available_loads_source_sql
    elif loads_type == 'Pre-pulled Loads':
        query = prepulled_loads_source_sql
    elif loads_type == 'Delivered Loads':
        query = delivered_loads_source_sql
         


    dfs=[]
    for params in sql_params_train_list:
        df= pd.read_sql(query, engine, params=params)
        dfs.append(df)


    # Combine all DataFrames into one using pd.concat() for the union
    df_train = pd.concat(dfs, ignore_index=True)



    df_test = pd.read_sql(query, engine, params=sql_params_test)




    # Separate features and target
    X_train = df_train.drop(['next_move', 'reference_number'], axis=1)  
    y_train = df_train['next_move']  

    X_test = df_test.drop(['next_move', 'reference_number'], axis=1)  
    y_test = df_test['next_move']  

    # Identify categorical columns
    categorical_columns = X_train.select_dtypes(include=['object']).columns

    # Initialize label encoder for categorical columns
    label_encoders = {}

    # Apply Label Encoding to categorical columns in both training and test sets
    for col in categorical_columns:
        label_encoders[col] = LabelEncoder()
        # Fit on combined training and test set to handle all categories
        label_encoders[col].fit(list(X_train[col]) + list(X_test[col]))  
        X_train[col] = label_encoders[col].transform(X_train[col])
        X_test[col] = label_encoders[col].transform(X_test[col])

    # Label encode the target variable
    label_encoder = LabelEncoder()
    # Fit on combined y_train and y_test to handle unseen labels
    label_encoder.fit(list(y_train) + list(y_test))

    y_train = label_encoder.transform(y_train)  # Transform training target
    y_test = label_encoder.transform(y_test)    # Transform test target

    # Convert the data into DMatrix for XGBoost
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)

    # Set the parameters for XGBoost
    params = {
        'objective': 'multi:softprob',  # For multi-class probability outputs
        'eval_metric': 'mlogloss',      # Use multi-class log loss
        'eta': 0.1,                     # Learning rate
        'max_depth': 6,                 # Max depth of the trees
        'num_class': len(np.unique(y_train)),  # Number of unique classes
        'random_state': 42
    }

    # Train the XGBoost model
    model = xgb.train(params, dtrain, num_boost_round=100)

    # Make predictions (probabilities for each class)
    y_pred_prob = model.predict(dtest)

    # Convert probabilities to class predictions by selecting the class with the highest probability
    y_pred = [np.argmax(probabilities) for probabilities in y_pred_prob]

    # Convert numerical predictions and actual test labels back to string labels (if necessary)
    y_pred = label_encoder.inverse_transform(y_pred)
    y_test = label_encoder.inverse_transform(y_test)

    y_pred_series = pd.Series(y_pred, name='Predicted')
    X_input = df_test.drop(['next_move', ], axis=1)  
    df_actions = pd.concat([y_pred_series, X_input], axis=1)
    df_dispatch = df_actions[df_actions['Predicted'] != 'Do Nothing']



    # Evaluate the model
    print ('Classification Report for '+loads_type)
    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("Classification Report:\n", classification_report(y_test, y_pred, zero_division=0))


    return df_dispatch  

DispatchPlan=pd.DataFrame()
for set in ['Available Loads', 'Pre-pulled Loads',  'Delivered Loads']:
    Plan = next_move_classifier(set, 'RoadEx', '2024-08-26')
    DispatchPlan = pd.concat([DispatchPlan, Plan], ignore_index=True)

print('Recommended Dispatch Plan is Ready!')
