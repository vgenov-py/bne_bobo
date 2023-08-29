from flask import Flask, render_template, request
from flaskwebgui import FlaskUI

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

if __name__ == "__main__":
    # app.run(debug=True)
    FlaskUI(app=app, server="flask").run()
