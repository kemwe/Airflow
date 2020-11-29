from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd 

from datetime import datetime,timedelta
import pandas as pd
import wget
import zipfile

#function for downloading data
def datadownload():
	url = 'https://storage.googleapis.com/kaggle-competitions-data/kaggle-v2/10118/111043/bundle/archive.zip?GoogleAccessId=web-data@kaggle-161607.iam.gserviceaccount.com&Expires=1606897698&Signature=C%2B3h79j0x6FQ222CDPXIsAMUwc0K1scVTKHAhNE42%2FEm8RWIfy%2Bjpo%2FUQY4u3qpJegJIORyxqYO7o%2BQR0XlESz9FNlDj6Oae4PLSc7wTAbicWaHbT0%2B6F9wVZOovaMKi1wONr8Ra0j0rTVCXbVxx6Lo537eYkQ%2BaSmj68ArA2FlvQGFAWlSOi4PQNSXWCsJkbEx8X9ILlkTIonufQC4bKkKdHjqHjqrRkBnQNQDwf6IWmz8TmUt3Uk8YA9jmLNDQNF%2FOus14o2%2BX9Yhf%2FP2Kd8Nk4XYCQU45vxx%2FXOKfDp4zip9u06Pfo%2FHE4y3Ggq1ZfJiGVs50RRi6nmLqh4roMQ%3D%3D&response-content-disposition=attachment%3B+filename%3Dtitanic-dataset.zip'
	path = '/mnt/c/dags/data'
	wget.download(url,out = path)

	return "Downloaded succesfully"


#unzipping the downloaded file
def unzipping():

	with zipfile.ZipFile("/mnt/c/dags/data/titanic-dataset.zip","r") as zip_ref:

		zip_ref.extractall("/mnt/c/dags/data")

    
#loading and cleaning the data(train.csv)
def loadcleaning():
	data=pd.read_csv("/mnt/c/dags/data/titanic_train.csv")
	#checking the missig values
	data.isnull().sum()
	#dropping variables with highest number of missing values
	data.drop(['cabin','boat','body','home.dest'],axis=1,inplace=True)
	#filling the missing data
	data['age'].fillna(value=data['age'].notnull().median(),inplace=True)
	data['fare'].fillna(value=data['fare'].notnull().mean(),inplace=True)
	data['embarked'].fillna(value=data['embarked'].notnull().mode(),inplace=True)

	return "succesfullycleaned"




default_args={
	'owner':'kennedy',
	'start_date':datetime(2020,11,29,10,00,00),
	'depends_on_past':False,
	'retries':0
}

with DAG(
		'my_test_dag',
		catchup=False,
	    default_args=default_args,
	    schedule_interval='*/5 * * * *'
	) as dag:
	
	downloaddata=PythonOperator(task_id='datadownload',
                               python_callable=datadownload)

	unzipfile=PythonOperator(task_id='unzipping',
                               python_callable=unzipping)

	loadclean=PythonOperator(task_id='loadcleaning',
                               python_callable=loadcleaning)

downloaddata>>unzipfile>>loadclean