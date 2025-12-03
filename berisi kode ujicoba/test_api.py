import requests
import csv

url = "https://script.google.com/macros/s/AKfycbxNu27V2Y2LuKUIQMK8lX1y0joB6YmG6hUwB1fNeVbgzEh22TcDGrOak03Fk3uBHmz-/exec?route=device-list"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()   # langsung list of devices

    if isinstance(data, list) and len(data) > 0:

        filename = "gsmarena_device_list.csv"
        headers = data[0].keys()  # brand_id, device_id, device_name, ...

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)

        print(f"Berhasil disimpan ke {filename}")
    else:
        print("Device list kosong.")
else:
    print("Request gagal:", response.status_code)
