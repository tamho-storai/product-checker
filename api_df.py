from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

@app.route('/process_data', methods=['POST'])
def process_data():
    data = request.json  # Nhận dữ liệu từ yêu cầu HTTP

    if 'filename' in data:
        filename = data['filename']
        df = pd.read_csv(filename) 

        df['UPC'] = df['UPC'].str.replace("'","").astype(str)
        df['STORE_CODE'] = df['STORE_CODE'].astype(str)
        df['PRICE'] = df['PRICE'].astype(int)
        df = df[['UPC','STORE_CODE','PRICE']]

        # Lưu DataFrame vào biến toàn cục để sử dụng sau này
        global df_global
        df_global = df

        # Chuyển đổi DataFrame thành cấu trúc dữ liệu có thể chuyển đổi thành JSON
        result = df.to_dict(orient='records')

        return jsonify({"message": "Data processed successfully", "data": result})
    else:
        return jsonify({"message": "Error: 'filename' not found in request data"}), 400


@app.route('/get_data_by_store_code', methods=['POST'])
def get_data_by_store_code():
    data = request.json  # Nhận dữ liệu từ yêu cầu HTTP

    if 'store_code' in data:
        store_code = data['store_code']

        # Lọc dữ liệu theo store_code
        filtered_df = df_global[df_global['STORE_CODE'] == store_code]

        # Chọn các cột cần trả về
        result = filtered_df[['UPC', 'STORE_CODE', 'PRICE']].to_dict(orient='records')

        return jsonify({"message": f"Data for store_code {store_code}", "data": result})
    else:
        return jsonify({"message": "Error: 'store_code' not found in request data"}), 400

if __name__ == '__main__':
    app.run(debug=True)