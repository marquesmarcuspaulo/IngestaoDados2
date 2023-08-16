import modin.pandas as pd
import boto3
import urllib.parse
import tempfile
import os
from io import StringIO
import ray
    
    
ray.init()

os.environ["MODIN_CPUS"] = "3"

session = boto3.Session(
    aws_access_key_id='ASIATFZPV6RYRN6YK3TJ',
    aws_secret_access_key='5HQF+Kl5s8HO45Cb6j3+qiTG7GUcz5vLZuVdBXFl',
    aws_session_token='FwoGZXIvYXdzELz//////////wEaDJJZ029MLHjHBsGqGiLOARPtD9aENqcSB+6awN78ZzN9YRvQZLuovDxoXd/177jz1PSrM+yYYCRoW5GwjsMSLt5m+9OgGmCguLMOGby/g+1z+fVTwmfU05vuIpAJQHc76n+KczfTmGbo+ULIm9D/o2SKlK0vgzs2Hn5TfXinJ70+GgbVIY08F4JusiqrAPPuuUvFQL4ctz1p1+HXF5SSzJw6JAQrMkrG8PkZvLrqkrNvIO9/mG5gyO+jRz1Phr6RBDGuNim321/LY9juFWctoFt3fr6hP8I50TyfdyoWKI+49KYGMi19piILSEI4p8WrFwi4Le4oDF4Tdz22H/hfVwVn+/N5HnxJfx9gVf1VLk0TU2w='
)


s3 = session.client('s3')

bucket_name = '218606466161-entrada'

bucket_output = '218606466161-saida'

csv_buffer = StringIO()

# Função para carregar o arquivo CSV a partir do S3
def load_csv_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_content = response['Body'].read().decode('utf-8')  # Decodificar em UTF-8
    return csv_content

# Função para criar DataFrame a partir do CSV e arquivo temporário
def create_dataframe_from_csv(csv_content, delimiter):
    with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8') as temp_file:
        temp_file.write(csv_content)
        temp_filename = temp_file.name
    df = pd.read_csv(temp_filename, sep=delimiter)
    os.remove(temp_filename)
    return df


def main():


    # Pasta no S3 para os arquivos de Empregados
    empregados_folder_path = 'Dados/Empregados/'
    # Pasta no S3 para os arquivos de Reclamações
    reclamacoes_folder_path = 'Dados/Reclamações/'
    bancos_folder_path = 'Dados/Bancos/'

    # Listar objetos na pasta de Empregados
    response_empregados = s3.list_objects(Bucket=bucket_name, Prefix=empregados_folder_path)
    csv_filenames_empregados = [obj['Key'] for obj in response_empregados.get('Contents', []) if obj['Key'].endswith('.csv')]

    # Listar objetos na pasta de Reclamações
    response_reclamacoes = s3.list_objects(Bucket=bucket_name, Prefix=reclamacoes_folder_path)
    csv_filenames_reclamacoes = [obj['Key'] for obj in response_reclamacoes.get('Contents', []) if obj['Key'].endswith('.csv')]

    response_bancos = s3.list_objects(Bucket=bucket_name, Prefix=bancos_folder_path)
    csv_filenames_bancos = [obj['Key'] for obj in response_bancos.get('Contents', []) if obj['Key'].endswith('.tsv')]


    # Lista para armazenar DataFrames de Empregados
    dfs_empregados = []

    # Carregar os conteúdos dos arquivos de Empregados
    for csv_filename in csv_filenames_empregados:
        csv_content = load_csv_from_s3(bucket_name, csv_filename)
        df1 = create_dataframe_from_csv(csv_content, delimiter='|')  # Usar | como delimitador
        dfs_empregados.append(df1)

    # Concatenar os DataFrames de Empregados
    empregados_df = pd.concat(dfs_empregados)

    # Agrupar os empregados pelo campo de agrupamento apropriado e somar os campos específicos
    grouped_empregados_df = empregados_df.groupby('Nome').agg({
        'Perspectiva positiva da empresa(%)': 'mean',
        'Remuneração e benefícios': 'mean'
    }).reset_index()


    grouped_empregados_df.rename(columns={
        'Perspectiva positiva da empresa(%)': 'Satisfação do Empregado',
        'Remuneração e benefícios': 'Satisfação Salário'
    }, inplace=True)


    dfs_reclamacoes = []

    for csv_filename in csv_filenames_reclamacoes:
        csv_content = load_csv_from_s3(bucket_name, csv_filename)
        df2 = create_dataframe_from_csv(csv_content, delimiter=';')
        dfs_reclamacoes.append(df2)

    concatenated_dfs_reclamacoes = pd.concat(dfs_reclamacoes)




    concatenated_dfs_reclamacoes.rename(columns={'Quantidade total de clientes – CCS e SCR': 'Quantidade total de clientes'}, inplace=True)

    # Converter colunas relevantes para os tipos de dados corretos
    concatenated_dfs_reclamacoes['Quantidade total de reclamações'] = concatenated_dfs_reclamacoes['Quantidade total de reclamações'].astype(int)

    # Converter a coluna 'Quantidade total de clientes' para int, substituindo valores vazios por NaN
    concatenated_dfs_reclamacoes['Quantidade total de clientes'] = pd.to_numeric(concatenated_dfs_reclamacoes['Quantidade total de clientes'], errors='coerce').astype('Int64')

    # Agrupar e agregar os DataFrames de reclamações
    grouped_reclamacoes_df = concatenated_dfs_reclamacoes.groupby('Instituição financeira').agg({
        'Índice': 'min',
        'Quantidade total de reclamações': 'sum',
        'Quantidade total de clientes': 'mean'
    }).reset_index()

    # Remover "(conglomerado)" do campo 'Instituição financeira'
    grouped_reclamacoes_df['Instituição financeira'] = grouped_reclamacoes_df['Instituição financeira'].str.replace('\(conglomerado\)', '', regex=True)

    # Remover espaços em branco do início e fim da string
    grouped_reclamacoes_df['Instituição financeira'] = grouped_reclamacoes_df['Instituição financeira'].str.strip()

    grouped_reclamacoes_df.rename(columns={'Instituição financeira': 'Nome'}, inplace=True)


    dfs_bancos = []

    for csv_filename in csv_filenames_bancos:
        csv_content = load_csv_from_s3(bucket_name, csv_filename)
        df3 = create_dataframe_from_csv(csv_content, delimiter='\t')  # Usar '\t' como delimitador
        dfs_bancos.append(df3)

    # Concatenar os DataFrames de Bancos
    bancos_df = pd.concat(dfs_bancos)

    # Remover "- PRUDENCIAL" do campo 'Nome'
    bancos_df['Nome'] = bancos_df['Nome'].str.replace('- PRUDENCIAL', '').str.strip()

    bancos_final_df = grouped_empregados_df.merge(grouped_reclamacoes_df, on='Nome', how='inner')
    bancos_final_df = bancos_final_df.merge(bancos_df, on='Nome', how='inner')


    bancos_final_df.to_csv(csv_buffer, index=False)

    output_filename = 'bancos_final.csv'

    output_key = output_filename

    if s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_output, Key=output_key):
        print(f'Arquivo {output_filename} salvo no S3 com sucesso!')
    else:
        print(f'Arquivo {output_filename} não foi salvo no S3. Atenção!')
        
if __name__ == '__main__':
    main()
