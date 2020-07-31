from datetime import timedelta, datetime
import urllib3
import os
import shutil
import lzma


def download_files(ds, folder, file, date_range, base_url, **kwargs):
    print(f"Date range: {date_range}")
    print(f"Base URL: {base_url}")
    print(f"Filename: {file}")

    sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
    edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        datareferencia = day.replace(day=1).strftime("%Y-%m")

        url = f"{base_url}{download_file_day}_{file}"
        print(f"Downloading: {url}")

        base_folder = f"data/staging/{datareferencia}/{folder}"

        os.makedirs(base_folder, exist_ok=True)

        fd = f"data/staging/{datareferencia}/{folder}/{download_file_day}_{file}"

        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)

        with open(fd, 'wb') as out:
            while True:
                data = r.read()
                if not data:
                    break
                out.write(data)

        r.release_conn()

    return "download realizado com sucesso"


def decompress_files(ds, folder, file, date_range, base_url, **kwargs):
    print(f"Date range: {date_range}")
    print(f"Base URL: {base_url}")
    print(f"Filename: {file}")

    sdate = datetime.strptime(date_range['date_start'], "%Y-%m-%d")
    edate = datetime.strptime(date_range['date_end'], "%Y-%m-%d")

    delta = edate - sdate

    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        download_file_day = day.strftime("%Y_%m_%d")

        datareferencia = day.replace(day=1).strftime("%Y-%m")

        base_folder = f"data/raw/{datareferencia}/{folder}"

        os.makedirs(base_folder, exist_ok=True)

        try:
            fstaging = f"data/staging/{datareferencia}/{folder}/{download_file_day}_{file}"
            fraw = f"{base_folder}/{download_file_day}_{file.replace('.xz', '')}"

            binary_data_buffer = lzma.open(fstaging, mode='rt', encoding='utf-8').read()

            with open(fraw, 'w') as a:
                a.write(binary_data_buffer)

        except Exception as err:
            print(f"Can't open file: {file} for date {download_file_day}")


def delete_files(ds, **kwargs):
    base_folder = f"data/staging/"
    shutil.rmtree(base_folder, ignore_errors=True)
