import json
import pymysql
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    db_secret=get_secret()
    #db_secret is a dictionary 
    try:
        con=pymysql.connect(host=db_secret['host'],database=db_secret['dbname'],user=db_secret['username'],passwd=db_secret['password'])
        #Above line establishes the connection with the db
        cursor=con.cursor()
        print("Connection Established with database")
        #employees table has already been created. 
        #If you want to create new table uncomment following line
        #cursor.execute("create table <table name> (eno int primary key,ename varchar(10),esal double(10,2),eaddr varchar(10))")
        eno=event['eno']
        ename=event['ename']
        esal=float(event['esal'])
        eaddr=event['eaddr']
        query = "INSERT INTO employees (eno, ename, esal, eaddr) VALUES (%s, %s, %s, %s)"
        params = (eno, ename, esal, eaddr)
        cursor.execute(query, params)
        con.commit()
            
    except pymysql.DatabaseError as e:
        if con:
            con.rollback()
            print("there is a problem", e)
    finally:
        if cursor:
            cursor.close()
        if con:
            con.close()
    
    # TODO implement
    return {
        'statusCode': 200,
            'body': json.dumps('Records inserted successfully')
    }
#This is function to fetch the secrets from AWS secret manager.    
def get_secret():
    secret_name = "dbsecret" #Name of the secret given in AWS
    region_name = "ap-south-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secr = get_secret_value_response['SecretString']
    #secr is a string. 
    #Following line converts it into a dictionary
    db_secret = json.loads(secr)
    return db_secret
