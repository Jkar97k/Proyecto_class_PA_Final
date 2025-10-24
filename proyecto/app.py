from flask import Flask

app = Flask(__name__)  # ⬅️ Cambio 1: 'flask' debe ser 'Flask'

@app.route('/Vamos')
def vamos():
    return 'nos vamossssssssssss'

@app.route('/')
def hola():
    return 'hola mundo'

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=5001)
    