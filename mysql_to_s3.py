import pymysql
import csv
import boto3
import configparser

# Lê o arquivo de configuração
parser = configparser.ConfigParser()
parser.read("pipeline.conf")

# Pega as infos do MySQL
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

# Conecta no MySQL
conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=int(port))

if conn is None:
    print("Erro na conexão com o banco MySQL")
else:
    print("Conexão com MySQL estabelecida!")

# Executa a query na tabela Pets
m_query = "SELECT * FROM Pets;"
local_filename = "pets_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

# Escreve em um arquivo CSV
with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()

# Pega as credenciais da AWS
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# Sobe o arquivo no S3
s3 = boto3.client('s3',
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key)

s3.upload_file(local_filename, bucket_name, local_filename)

print("Arquivo enviado para o S3 com sucesso!")

