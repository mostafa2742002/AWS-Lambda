import requests
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_ENDPOINT = os.getenv("DB_ENDPOINT")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Constants for database connection and URLs
DB_NAME = 'ec2_instances'
DB_ENDPOINT = 'free-tier-database.c5x2u38ouo1m.eu-north-1.rds.amazonaws.com'
DB_PORT = 5432
DB_USER = 'admin'
DB_PASSWORD = 'awsproject123'

# Define URL templates for different regions
the_base_url = "https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/EU%20{region}/Linux/index.json?timestamp={timestamp}"
REGION_URLS = {
    ["(Frankfurt)", the_base_url.format(region="Frankfurt", timestamp='1695336606682')],
    ["(Ireland)", the_base_url.format(region="Ireland", timestamp='1695336640824')],
    ["(London)", the_base_url.format(region="London", timestamp='1695336671834')],
    ["(Milan)", the_base_url.format(region="Milan", timestamp='1695336709113')],
    ["(Paris)", the_base_url.format(region="Paris", timestamp='1695336734334')],
    ["(Spain)", the_base_url.format(region="Spain", timestamp='1695336756525')],
    ["(Stockholm)", the_base_url.format(region="Stockholm", timestamp='1695336795677')],
    ["(Zurich)", the_base_url.format(region="Zurich", timestamp='1695336817871')],
}

def convert(url, region_name):
    response = requests.get(url)
    data_info = response.json()

    data = data_info.get("regions", {})
    data = data['EU (Frankfurt)']
    
    if response.status_code == 200:
        instances = []
        for instance_name, instance_attributes in data.items():
            instance = {
                'Instance Name': instance_name,
                'Rate Code': instance_attributes.get('rateCode', ''),
                'Price': instance_attributes.get('price', ''),
                'Location': instance_attributes.get('Location', ''),
                'Instance Family': instance_attributes.get('Instance Family', ''),
                'vCPU': instance_attributes.get('vCPU', ''),
                'Memory': instance_attributes.get('Memory', ''),
                'Storage': instance_attributes.get('Storage', ''),
                'Network Performance': instance_attributes.get('Network Performance', ''),
                'Operating System': instance_attributes.get('Operating System', ''),
                'Pre Installed S/W': instance_attributes.get('Pre Installed S/W', ''),
                'License Model': instance_attributes.get('License Model', ''),
            }
            instances.append(instance)

        return instances
    else:
        return None


def insert_data(sql, conn, parameters=[]):
    cur = conn.cursor()
    cur.execute(sql, parameters)
    conn.commit()
    cur.close()

def select_data(sql, conn, parameters=[]):
    cur = conn.cursor()
    cur.execute(sql, parameters)
    result = cur.fetchone()
    if result:
        if sql.startswith("SELECT region_id"):
            parameters["region_id"] = result[0]
        elif sql.startswith("SELECT operating_system_id"):
            # we want the last element in the tuple
            parameters["operating_system_id"] = result[0]
        elif sql.startswith("SELECT vcpu_id"):
            parameters["vcpu_id"] = result[0]
        elif sql.startswith("SELECT instance_id"):
            parameters["instance_id"] = result[0]

    conn.commit()
    
    cur.close()

def save_data(instance_name, instance_attributes, region_name, conn):
    # Extract data from instance_attributes
    memory = float(instance_attributes.get('Memory', '').replace(' GiB', ''))
    storage = instance_attributes.get('Storage', '')
    network_performance = instance_attributes.get('Network Performance', '')
    operating_system_name = instance_attributes.get('Operating System', '')
    vcpu_cores_count = int(instance_attributes.get('vCPU', ''))
    price = float(instance_attributes.get('Price', '').replace('$', ''))

    parameters = {
        "region_name": region_name,
        "os_name": operating_system_name,
        "core_count": vcpu_cores_count,
        "instance_name": instance_name,
        "memory": memory,
        "storage": storage,
        "network_performance": network_performance,
        "price": price,
        "region_id": None,
        "operating_system_id": None,
        "vcpu_id": None,
        "instance_id": None
    }

    sql_statements = {
        "insert_region_sql": "INSERT INTO regions (region_long_name) VALUES (:region_name)",
        "insert_os_sql": "INSERT INTO operating_systems (operating_system_name) VALUES (:os_name)",
        "insert_vcpu_sql": "INSERT INTO vcpu_cores (core_count) VALUES (:core_count)",
        "select_region_sql": "SELECT region_id FROM regions WHERE region_long_name = :region_name",
        "select_os_sql": "SELECT operating_system_id FROM operating_systems WHERE operating_system_name = :os_name ORDER BY operating_system_id DESC LIMIT 1",
        "select_vcpu_sql": "SELECT vcpu_id FROM vcpu_cores WHERE core_count = :core_count",
        "insert_instance_sql": "INSERT INTO ec2_instances (vcpu_id, memory, storage, network_performance, operating_system_id, instance_name) VALUES (:vcpu_id, :memory, :storage, :network_performance, :operating_system_id, :instance_name)",
        "select_instance_sql": "SELECT instance_id FROM ec2_instances WHERE instance_name = :instance_name",
        "insert_region_instance_sql": "INSERT INTO region_instances (region_id, instance_id, price_per_hour) VALUES (:region_id, :instance_id, :price)"
    }

    for sql_key, sql_query in sql_statements.items():
        if sql_key.startswith("insert"):
            insert_data(sql_query, conn, parameters)
        elif sql_key.startswith("select"):
            select_data(sql_query, conn, parameters)


def fetch_data(region_name, url , conn):
    data = convert(url, region_name)

    if data:
        for instance in data:
            save_data(instance['Instance Name'], instance, region_name , conn)
    else:
        return None


def main():
    # in develop
    try:
        conn = psycopg2.connect(
            host=DB_ENDPOINT,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        for region_name, url in REGION_URLS:
            fetch_data(region_name, url, conn)

        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
