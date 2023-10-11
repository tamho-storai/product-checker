import requests
import pymsteams
import os
import pandas as pd
from decouple import config
from datetime import datetime
from requests.exceptions import RequestException
from azure.storage.blob import BlobServiceClient

# Define your connection string and container and blob names
BLOB_SERVICE_CLIENT= BlobServiceClient.from_connection_string(config("AZURE_STORAGE_CONNECTION_STRING"))
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(config("CONTAINER_NAME"))

def download_csv_file_from_blob(container_client, blob_name, local_data_dir):

    if blob_name.endswith('.csv'):
        local_file_name = os.path.join(local_data_dir, 'price_sale_of_product.csv')
        blob_client = container_client.get_blob_client(blob_name)

        with open(local_file_name, "wb") as data:
            blob_data = blob_client.download_blob()
            blob_data.readinto(data)

        print(f"  ===> Downloaded {blob_name} to {local_file_name}")
    else:
        print(f"  ---> Skipped {blob_name} as it is not a .csv file")

def read_csv_to_df(filename):
   
    df = pd.read_csv(filename) 

    df['UPC'] = df['UPC'].str.replace("'","").astype(str)
    df['STORE_CODE'] = df['STORE_CODE'].astype(str)
    df['PRICE'] = df['PRICE'].astype(int)
    df=df[['UPC','STORE_CODE','PRICE']]

    return df

def get_sale_price(upc, store_code, url, headers, body):
    try:
        response = requests.post(url, headers=headers, json=body)

        # Check if the request was successful (status code 200)
        response.raise_for_status()

        data = response.json()

        if 'Products' in data and data['Products']:
            sale_price_ecom = int(data['Products'][0]['SalePrice'])
            return sale_price_ecom
        
        elif 'Items' in data and data['Items']:
            sale_price_product = int(data['Items'][0]['SalePrice'])
            return sale_price_product
        
        else:
            print(f"No sale price data returned for UPC {upc} and Store Code {store_code}")
            return "None"

    except RequestException as e:
        print(f"Error fetching sale price for UPC {upc} and Store Code {store_code}: {e}")
        return "None"

def compare_sale_price(price1, price2):
    return price1 == price2

def send_notification(yyyymmdd, upc, mess, success=True):

    myTeamsMessage = pymsteams.connectorcard(config('WEB_HOOK'))
    myTeamsMessage.title("Notification from QC-AUTOMATION-TESTING")
    section = pymsteams.cardsection()

    if success:
        section.title("✅ Successful")
    else:
        section.title("🔴 Some Sale_prices are not matching")

    section.activityTitle("Testing Details")
    section.addFact("UPC  :", f"{upc} ")
    section.addFact("Error", f"{mess}")
    section.addFact("Date", str(yyyymmdd.strftime('%Y-%m-%d')))

    myTeamsMessage.addSection(section)
    myTeamsMessage.text("Summary of the notification")
    myTeamsMessage.send()

def main():

    print("")

    current_time = datetime.now()
    yyyymmdd=current_time.strftime('%Y%m%d')

    local_data_dir = "data"
    blob_name = f"{yyyymmdd}/price_sale_of_product.csv"

    download_csv_file_from_blob(CONTAINER_CLIENT, blob_name, local_data_dir)

    filename="data/price_sale_of_product.csv"

    df=read_csv_to_df(filename)

    print(df)

    total_rows = df.shape[0]

    print(f"Data has : {total_rows} rows")

    url_ecom = config("URL_ECOM")
    url_product = config("URL_PRODUCT")

    headers_ecom = {'content-type': "application/json; charset=UTF-8"}
    headers_product = {'content-type': 'application/json'}

    count=0

    for row in df.iterrows():

        count=count+1
    
        upc = row[1]['UPC']
       
        store_code = row[1]['STORE_CODE']
     
        sale_price = row[1]['PRICE']
        

        body_ecom = {
        "Keyword": upc,
        "StoreID": store_code,
        "PageSize": 1,
        "PageNum": 1,
        }

        body_product = {
        "Keyword": upc,
        "PageSize": 1,
        "PageNum": 1,
        "filters": [{"type": 0, "fieldName": "StoreCode", "values": [store_code]}]
        }

        sale_price_product = get_sale_price(upc, store_code, url_product, headers_product, body_product)
        sale_price_ecom = get_sale_price(upc, store_code, url_ecom, headers_ecom, body_ecom)

        print(" ")
        print(f"UPC: {upc:<20}STORE_CODE: {store_code:<10}Sale_price: {sale_price:<10}Sale_price_product: {sale_price_product:<10}Sale_price_ecom: {sale_price_ecom:<10}")

        info = {
        "store_code": store_code,
        "sale_price": sale_price,
        "sale_price_product": sale_price_product,
        "sale_price_ecom": sale_price_product
        }

        n=0
        if count==1000:
            n=n+1

            print(f"===>    Done {n}k rows")

            count=0

        if not compare_sale_price(sale_price_product, sale_price_ecom) or not compare_sale_price(sale_price_product, sale_price):
            print(" ")
            mess=f"UPC: {upc:<20}STORE_CODE: {store_code:<10}Sale_price: {sale_price:<10}Sale_price_product: {sale_price_product:<10}Sale_price_ecom: {sale_price_ecom:<10}"
            print(mess)

    mess=f"No errors"
    print(f"===>    Done {total_rows} rows")
    send_notification(current_time,upc,mess,success=True)

if __name__ == '__main__':
    main()


