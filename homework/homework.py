import os
import pandas as pd
import zipfile

"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    input_directory = 'files/input'

    files = [f for f in os.listdir(input_directory) if f.endswith('.zip')]

    dfs = []

    for file in files:
        zip_path = os.path.join(input_directory, file)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_file = zip_ref.namelist()[0]
            with zip_ref.open(csv_file) as f:
                df = pd.read_csv(f)
                dfs.append(df)

    dataset = pd.concat(dfs, ignore_index=True)


    dataset['job'] = dataset['job'].str.replace('.' , '').str.replace('-' , '_')
    dataset['education'] = dataset['education'].str.replace('.' , '_')
    dataset['education'] = dataset['education'].apply(lambda x: pd.NA if x == 'unknown' else x)
    dataset['credit_default'] = dataset['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    dataset['mortgage'] = dataset['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
    dataset['previous_outcome'] = dataset['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    dataset['campaign_outcome'] = dataset['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    dataset['month'] = pd.to_datetime(dataset['month'], format='%b').dt.month
    dataset['last_contact_date'] = pd.to_datetime({'year':2022, 'month':dataset['month'], 'day':dataset['day']})

    client = dataset[
    ['client_id', 
    'age',
    'job',
    'marital',
    'education',
    'credit_default',
    'mortgage']]

    campaign = dataset[
        ['client_id',
        'number_contacts',
        'contact_duration',
        'previous_campaign_contacts',
        'previous_outcome',
        'campaign_outcome',
        'last_contact_date']]

    economics = dataset[
        ['client_id',
        'cons_price_idx',
        'euribor_three_months']]


    os.makedirs('files/output/', exist_ok=True)

    output_directory = 'files/output/'
    dataframes = {'client': client, 
                'campaign': campaign, 
                'economics': economics}

    for name, df in dataframes.items():
        df.to_csv(f'{output_directory}{name}.csv', index=False)


    return 1


if __name__ == "__main__":
    clean_campaign_data()
