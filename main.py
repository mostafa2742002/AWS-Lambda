import json
import os
import mysql.connector
from mysql.connector import Error
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

#with open('urls_and_table.json', 'r') as file:
#    tables_items = json.load(file)


def convert(url, region_name):
    print("Convert URL = " + url)
    response = requests.get(url)
    
    data_info = response.json()
    data = data_info.get("regions", {})
    region_name = region_name.replace('Europe', 'EU')
    data = data[region_name]
    if response.status_code == 200:
        instances = []
        for instance_name, instance_attributes in data.items():
            instance = {'Instance Name': instance_name}
            for i in ["rateCode", "price", "Location", "Instance Family", "vCPU", "Memory", "Storage", "Network Performance", "Operating System", "Pre Installed S/W", "License Model"]:
                instance.update({i: instance_attributes.get(i, '')})
            instances.append(instance)
        return instances
    else:
        return None


def insert_data(sql, conn, parameters=[]):
    cur = conn.cursor()
    cur.execute(sql, parameters)
    conn.commit()
    cur.close()


def get_value(sql, conn, parameters=[]):
    cur = conn.cursor()
    cur.execute(sql, parameters)
    result = cur.fetchone()[0]
    
    cur.close()
    return result

def save_region_data(region_name, conn):
    print("Saving Region Data for region_name = " + region_name)
    parameters =(region_name, region_name, region_name)
    sql_query = "INSERT INTO regions (region_long_name, region_short_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE region_long_name = %s"
    insert_data(sql_query, conn, parameters)
    print("Finished saving region data")

def save_os_data(operating_system_name, conn):
    print("Saving OS Data for OS Name = " + operating_system_name)
    parameters = (operating_system_name , operating_system_name)
    sql_query = "INSERT INTO operating_systems (operating_system_name) VALUES (%s) ON DUPLICATE KEY UPDATE operating_system_name = %s"
    insert_data(sql_query, conn, parameters)

def save_vcpu_data(core_count, conn):
    print("Saving VCPU Data for Core Count = " + core_count)
    parameters = (core_count, core_count)
    sql_query = "INSERT INTO vcpu_cores (core_count) VALUES (%s) ON DUPLICATE KEY UPDATE core_count = %s"
    insert_data(sql_query, conn, parameters)

def save_ec2_instance_data(instance, conn):
    print("Saving EC2 Instances Data for instance name = " + instance['Instance Name'])
    parameters = (instance['vCPU'],) 
    vcpu_query = "SELECT vcpu_core_id FROM vcpu_cores WHERE core_count = %s"
    vcpu_id = get_value(vcpu_query, conn, parameters)
    parameters = (instance['Operating System'],)
    os_query = "SELECT operating_system_id FROM operating_systems WHERE operating_system_name = %s"
    os_id = get_value(os_query, conn, parameters)
    
    parameters = (vcpu_id, instance['Memory'], instance['Storage'], instance['Network Performance'], os_id, instance['Instance Name'], vcpu_id, instance['Memory'], instance['Storage'], instance['Network Performance'], os_id, instance['Instance Name']) 
    sql_query = "INSERT INTO ec2_instances (vcpu_core_id, memory, storage, network_performance, operating_system_id, instance_name) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE vcpu_core_id = %s, memory = %s, storage = %s, network_performance = %s, operating_system_id = %s , instance_name = %s"
    insert_data(sql_query, conn, parameters)

def save_prices(instance_name, region_name, price, conn):
    print("Saving Prices for region = " + region_name)
    parameters = (region_name,) 
    region_id_query = "select region_id from regions where region_long_name = %s"
    region_id = get_value(region_id_query, conn, parameters)

    parameters = (instance_name,)
    instance_query = "select instance_id from ec2_instances where instance_name = %s"
    instance_id = get_value(instance_query, conn, parameters)

    parameters = (region_id, instance_id, price, price)
    region_instances_query = "insert into region_instances(region_id, instance_id, price_per_hour) values(%s, %s, %s) on duplicate key update price_per_hour = %s "
    insert_data(region_instances_query, conn, parameters)
    

def fetch_data(region_name, url, conn):
    print("Fetch Data for Region = " + region_name)
    data = convert(url, region_name)

    if data:
        for instance in data:
            print (instance['Operating System'] + " " + instance['vCPU'])
            save_region_data(region_name, conn)
            save_os_data(instance['Operating System'], conn)
            save_vcpu_data(instance['vCPU'], conn) 
            save_ec2_instance_data(instance, conn)
            save_prices(instance['Instance Name'], region_name, instance['price'], conn)
    else:
        return None


def web_scraping_engine():
    try:

        conn = mysql.connector.connect(
            host=DB_ENDPOINT,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        print("Connection Succeeded!")

        [url_prefix, url_suffix] = ["https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/EU%20(", ")/Linux/index.json?timestamp="]
        for [region_name, timestamp] in [["Frankfurt", "1695336606682"], ["Ireland", "1695336640824"], ["London", "1695336671834"], ["Milan", "1695336709113"], ["Paris", "1695336734334"], ["Spain", "1695336756525"], ["Stockholm", "1695336795677"], ["Zurich", "1695336817871"]]:
            region_name_fun = "Europe (" + region_name + ")"
            region_url_fun = url_prefix + region_name + url_suffix + timestamp
            fetch_data(region_name_fun, region_url_fun, conn)
        conn.close()

    except Exception as e:
        print(e)


def lambda_handler(event , context):
    web_scraping_engine()