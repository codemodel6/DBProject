def handleComment(data,i,j):
    columnComment = ""
    # 테이블 설명
    tableComment = data['TableData']["TableInfo"][i][j]["Comment"]['Table']
    # 각각의 컬럼 리스트
    columnCommentList = list(data['TableData']["TableInfo"][i][j]["Comment"]['Columns'])
    # 컬럼 리스트의 내용 가져오기
    for c in columnCommentList:
        column = data['TableData']["TableInfo"][i][j]["Comment"]['Columns'][c]
        columnComment += f" * {c} : {column}\n"
    # 합치기
    comment = f"/* {tableComment}\n{columnComment}*/\n\n"
    return comment
