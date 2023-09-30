import json
import sqlite3

import requests

# import boto3


with open('urls_and_table.json', 'r+') as f:
    tables_items = json.load(f)


def convert(url, region_name):
    response = requests.get(url)
    data_info = response.json()
    data = data_info.get("regions", {})
    region_name = region_name.replace('Europe', 'EU')
    data = data[region_name]
    if response.status_code == 200:
        instances = []
        for instance_name, instance_attributes in data.items():
            instance = {'Instance Name': instance_name}
            for i in tables_items['instance_attributes_get_items']:
                instance.update({i: instance_attributes.get(i, '')})
            instances.append(instance)
        return instances
    else:
        return None


def fetch_data(region_name, url, conn):
    data = convert(url, region_name)

    if data:
        for instance in data:
            save_data(instance['Instance Name'], instance, region_name, conn)
    else:
        return None


def save_data(instance_name, instance_attributes, region_name, conn):
    # Extract data from instance_attributes
    memory = float(instance_attributes.get('Memory', '').replace(' GiB', ''))
    storage = instance_attributes.get('Storage', '')
    network_performance = instance_attributes.get('Network Performance', '')
    operating_system_name = instance_attributes.get('Operating System', '')
    vcpu_cores_count = int(instance_attributes.get('vCPU', ''))
    price = float(instance_attributes.get('price', '').replace('$', ''))

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


def main():
    conn = sqlite3.connect('test.db')
    print("Opened database successfully")

    [url_prefix, url_suffix] = tables_items['long_url']
    for [region_name, timestamp] in tables_items['URLS_RE']:
        region_name_fun = "Europe (" + region_name + ")"
        region_url_fun = url_prefix + region_name + url_suffix + timestamp
        fetch_data(region_name_fun, region_url_fun, conn)
        conn.close()


if __name__ == "__main__":
    main()
