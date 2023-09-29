import requests
import json
import psycopg2
import concurrent.futures

# Constants for database connection and URLs
DB_NAME = 'ec2_instances'
DB_ENDPOINT = 'free-tier-database.c5x2u38ouo1m.eu-north-1.rds.amazonaws.com'
DB_PORT = 5432
DB_USER = 'admin'
DB_PASSWORD = 'awsproject123'

# Define URL templates for different regions
the_base_url = "https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/EU%20{region}/Linux/index.json?timestamp={timestamp}"
REGION_URLS = {
    ["(Frankfurt)" , the_base_url.format(region="Frankfurt", timestamp='1695336606682')],
    ["(Ireland)" , the_base_url.format(region="Ireland", timestamp='1695336640824')],
    ["(London)" , the_base_url.format(region="London", timestamp='1695336671834')],
    ["(Milan)" , the_base_url.format(region="Milan", timestamp='1695336709113')],
    ["(Paris)" , the_base_url.format(region="Paris", timestamp='1695336734334')],
    ["(Spain)" , the_base_url.format(region="Spain", timestamp='1695336756525')],
    ["(Stockholm)" , the_base_url.format(region="Stockholm", timestamp='1695336795677')],
    ["(Zurich)" , the_base_url.format(region="Zurich", timestamp='1695336817871')],
}

def execute_sql(conn, sql, parameters=[]):
    cur = conn.cursor()
    cur.execute(sql, parameters)
    conn.commit()
    cur.close()

def save_data(instance_name, instance_attributes, region_name, conn):
    # Extract data from instance_attributes
    memory = float(instance_attributes.get('Memory', '').replace(' GiB', ''))
    storage = instance_attributes.get('Storage', '')
    network_performance = float(instance_attributes.get(
        'Network Performance', '').replace(' Gigabit', ''))
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
        "price": price
    }

    sql_statements = {
        "insert_region_sql": "INSERT INTO regions (region_long_name) VALUES (%s)",
        "insert_os_sql": "INSERT INTO operating_systems (operating_system_name) VALUES (%s)",
        "insert_vcpu_sql": "INSERT INTO vcpu_cores (core_count) VALUES (%s)",
        "select_region_sql": "SELECT region_id FROM regions WHERE region_long_name = %s",
        "select_os_sql": "SELECT operating_system_id FROM operating_systems WHERE operating_system_name = %s",
        "select_vcpu_sql": "SELECT vcpu_id FROM vcpu_cores WHERE core_count = %s",
        "insert_instance_sql": "INSERT INTO ec2_instances (vcpu_id, memory, storage, network_performance, operating_system_id, instance_name) VALUES (%s, %s, %s, %s, %s, %s)",
        "select_instance_sql": "SELECT instance_id FROM ec2_instances WHERE instance_name = %s",
        "insert_region_instance_sql": "INSERT INTO region_instances (region_id, instance_id, price_per_hour) VALUES (%s, %s, %s)"
    }

    for sql_statement in sql_statements.values():
        execute_sql(conn, sql_statement, parameters.values())


def fetch_data(region_name, url, conn):
    response = requests.get(url)
    if response.status_code == 200:
        data_info = response.json()
        region_data = data_info['regions'].get(region_name, {})
        instances = []
        for instance_name, instance_attributes in region_data.items():
            # Process and insert data into the database
            save_data(instance_name, instance_attributes, region_name, conn)
            instances.append(instance_name)
            break

        return instances
    else:
        return None


def main():
    try:
        conn = psycopg2.connect(
            host=DB_ENDPOINT,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_data, region_name, url, conn) for region_name, url in REGION_URLS.items()]

        concurrent.futures.wait(futures)

        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
