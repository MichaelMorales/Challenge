# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 19:13:30 2023

@author: Usuario Autorizado
"""

from flask import Flask, request, jsonify
app = Flask(__name__)
@app.route('/api/endpoint', methods=['GET'])
def get_data():
    # Lógica para manejar la solicitud GET
    data = {'message': '¡Hola desde el API!'}
    return jsonify(data)

if __name__ == '__main__':
    app.run()