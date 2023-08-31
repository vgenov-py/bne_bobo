from flask import Flask, render_template, request
from flaskwebgui import FlaskUI
from os import listdir
app = Flask(__name__)
@app.route("/", methods=["GET", "POST"])
def t_home():
    if request.method == "POST":
        try:
            import creator
            return render_template("create.html", msg="¡Base de datos creada correctamente!")
        except Exception as e:
            return render_template("create.html", msg=f"{e}")
            
    return render_template("create.html")

@app.route("/ckan", methods=["GET", "POST"])
def t_ckan():
    if request.method == "POST":
        try:
            import ckan2
            return render_template("ckan.html", msg="¡Ficheros creados correctamente!")
        except Exception as e:
            return render_template("ckan.html", msg=f"{e}")

    return render_template("ckan.html", is_db=True if "bne.db" in listdir("./") else False)

# @app.teardown_appcontext
# def close_connection(exception):
#     db = getattr(g, '_database', None)
#     if db is not None:
#         db.close()

if __name__ == "__main__":
    # app.run(debug=True)
    FlaskUI(app=app, server="flask").run()
