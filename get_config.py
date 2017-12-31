#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import Flask
from flask import request

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def get_conf():
    if request.method != 'POST':
        print('Definitely not a POST')
        return 'Not POST', 404
    else:
        # в случае POST-запроса берём данные из form
        print(request.form)

        # записываем текст в файл конфига
        config_data = request.form.get('data')
        with open('spark-defaults.conf', 'w') as f:
            f.write(config_data)
        return 'Yeah!', 200

if __name__ == '__main__':
    app.run(port=8080, debug=True)
