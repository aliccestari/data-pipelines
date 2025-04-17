import boto3
import configparser

# Carregar as credenciais do arquivo pipeline.conf
config = configparser.ConfigParser()
config.read('pipeline.conf')

aws_credentials = config['aws_boto_credentials']
access_key = aws_credentials['access_key']
secret_key = aws_credentials['secret_key']
bucket_name = aws_credentials['bucket_name']

# Criar uma sessão do boto3 com as credenciais
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Exemplo: Listar objetos dentro do bucket S3
try:
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Verificar se o bucket contém objetos
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"Object: {obj['Key']}, Last Modified: {obj['LastModified']}")
    else:
        print("O bucket está vazio.")
except Exception as e:
    print(f"Erro ao acessar o bucket S3: {e}")

# Fazer upload de um arquivo para o S3
try:
    s3.upload_file('dados.txt', bucket_name, 'dados_exemplo.txt')
    print("Arquivo carregado com sucesso!")
except Exception as e:
    print(f"Erro ao carregar o arquivo para o S3: {e}")

