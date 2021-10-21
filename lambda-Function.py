import boto3
import http.client
import base64
import ast
mwaa_env_name = 'meraki_mwaa_env'
dag_name = 'poc-meraki-airflow'
mwaa_cli_command = 'trigger_dag'

client = boto3.client('mwaa')

def lambda_handler(event, context):
    # get web token
    mwaa_cli_token = client.create_cli_token(
        Name=mwaa_env_name
    )
    
    conn = http.client.HTTPSConnection(mwaa_cli_token['WebServerHostname'])
    payload = "trigger_dag " + dag_name
    headers = {
      'Authorization': 'Bearer ' + mwaa_cli_token['CliToken'],
      'Content-Type': 'text/plain'
    }
    conn.request("POST", "/aws_mwaa/cli", payload, headers)
    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    return base64.b64decode(mydata['stdout'])
