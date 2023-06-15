from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse

app = FastAPI()
# Path = "C:/Users/kbkim/PycharmProjects/pythonProject/result.json"
Path = "C:/code/dbproject/Python/kkb/result.json"


@app.get("/")
def read_root():
    html_content = """
    <html>
        <head>
            <title>Some HTML in here</title>
            <style>
                .btn {width: 100px; 
                height: 100px; 
                cursor : pointer;
                background-color: #B9EDDD;
                color : #FEFF86;
                border: none;
                font-size: 20px;
                font-weight: 800;
                transition: transform .2s ease, padding .2s ease;
                display:inline-block;
                box-sizing: border-box
                }
                .btn:hover {background-color: #FEFF86; color: #B9EDDD;
                transform: translate(0,-20px);}
            </style>
        </head>
        <body>
            <h1>반가워요</h1>
            <a href="/files">
                <button class="btn">다운로드</button>
            </a>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content, status_code=200)


@app.get("/files")
async def give_file():
    return FileResponse(Path, media_type='text/txt', filename="result.json")