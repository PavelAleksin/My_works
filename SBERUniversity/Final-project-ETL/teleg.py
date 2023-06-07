#!/usr/bin/python3
#Не получилось настроить пересылку в чат не хватает библиотек
import requests
#Номер бота
TOKEN = "5804101843:AAHS41OjKt_fcJIKM9hHhYxaSEks3mF2At8"
#Номер чата бота
chat_id = "1071137776" 
#Открываем файл
f = open('/home/de12/alpa/project/rep.txt','r')
#Читаем файл перед отправкой
report = f.read()
#Отправка сообшения
message = report
url =f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
#Закрываем файл
f.close
requests.get(url).json()
print('Сообшение отправлено в чат Telegram  @Aaaaaaaaaaa69bot')
