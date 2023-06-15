from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse

app = FastAPI()
TXTFILEPATH = "./Files/"
TXTFILENAME = TXTFILEPATH+"A.txt"
JSONFILEPATH = "./Files/"
JSONFILENAME = TXTFILEPATH+"T.json"


@app.get("/")
def read_root():
    html_content = """
    <html>
        <head>
            <title>Some HTML in here</title>
        </head>
        <body>
            <h1>Helo World</h1>
            <a href="/GiveA">
                <input type="button" value="몰?루" />
            </a>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)

@app.get("/GiveA")
async def give_file():
    return FileResponse(TXTFILENAME, media_type='text/txt',filename="A.txt")

@app.get("/GiveT")
async def give_file():
    return FileResponse(JSONFILENAME, media_type='text/json', filename="T.JSON")