import schedule
import time
import requests
import daemon2
import dotenv, os
dotenv.load_dotenv()

MAIN_URL = os.getenv("MAIN_URL")
def get_data():

    response = requests.get(MAIN_URL+'news/')
    print(response.status_code)
    if response.status_code == 200:
        data = response.json()
        print(data)
        daemon2.insert_data(data)
        
            

schedule.every(10).seconds.do(get_data)

while True:
    schedule.run_pending()
    time.sleep(1)  # добавлен краткий интервал ожидания, чтобы избежать высокой загрузки процессора
